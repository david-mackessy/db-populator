package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.JsonValue;
import com.dbpopulator.model.TableMetadata;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BatchInsertService {

    private static final Logger log = LoggerFactory.getLogger(BatchInsertService.class);

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    @Value("${app.batch.size:1000}")
    private int batchSize;

    public BatchInsertService(DataSource dataSource, DataGeneratorService dataGenerator,
                              SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public int insertBatch(String tableName, int count, ProgressCallback callback) throws SQLException {
        log.debug("Starting batch insert for table: {}, count: {}", tableName, count);

        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            log.error("Cannot insert - table not found in schema cache: {}", tableName);
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        List<ColumnMetadata> insertableColumns = table.getInsertableColumns();
        if (insertableColumns.isEmpty()) {
            log.warn("No insertable columns for table: {} - skipping insert", tableName);
            return 0;
        }

        String sql = buildInsertSql(tableName, insertableColumns);
        log.debug("Generated SQL for {}: {}", tableName, sql);

        ColumnMetadata pkColumn = table.getPrimaryKeyColumn();
        boolean returnGeneratedKeys = pkColumn != null && pkColumn.isAutoIncrement();

        int totalInserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            log.debug("Acquired connection for table: {}", tableName);

            try (PreparedStatement ps = returnGeneratedKeys
                    ? conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
                    : conn.prepareStatement(sql)) {

                for (int i = 0; i < count; i++) {
                    Map<String, Object> row = dataGenerator.generateRow(table);
                    setParameters(ps, insertableColumns, row);
                    ps.addBatch();

                    if ((i + 1) % batchSize == 0 || i == count - 1) {
                        int[] results = ps.executeBatch();
                        int batchInserted = countInserted(results);
                        totalInserted += batchInserted;

                        if (returnGeneratedKeys && pkColumn != null) {
                            cacheGeneratedKeys(ps, table, pkColumn);
                        }

                        conn.commit();
                        ps.clearBatch();

                        if (callback != null) {
                            callback.onProgress(totalInserted, count);
                        }

                        log.debug("Inserted batch for {}: {}/{}", tableName, totalInserted, count);
                    }
                }

            } catch (SQLException e) {
                log.error("SQL error during batch insert for table: {}, rolling back. Error: {} - SQLState: {}",
                    tableName, e.getMessage(), e.getSQLState(), e);
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("Failed to acquire connection or complete transaction for table: {}", tableName, e);
            throw e;
        }

        log.info("Completed inserting {} rows into {}", totalInserted, tableName);
        return totalInserted;
    }

    private String buildInsertSql(String tableName, List<ColumnMetadata> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");

        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
                placeholders.append(", ");
            }
            sql.append(columns.get(i).name());
            placeholders.append("?");
        }

        sql.append(") VALUES (").append(placeholders).append(")");
        return sql.toString();
    }

    private void setParameters(PreparedStatement ps, List<ColumnMetadata> columns, Map<String, Object> row)
            throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            Object value = row.get(col.name());
            setParameter(ps, i + 1, value, col.sqlType());
        }
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int sqlType) throws SQLException {
        if (value == null) {
            ps.setNull(index, sqlType);
            return;
        }

        if (value instanceof UUID uuid) {
            ps.setObject(index, uuid);
        } else if (value instanceof JsonValue json) {
            PGobject pgObject = new PGobject();
            pgObject.setType("jsonb");
            pgObject.setValue(json.value());
            ps.setObject(index, pgObject);
        } else if (value instanceof String s) {
            ps.setString(index, s);
        } else if (value instanceof Integer i) {
            ps.setInt(index, i);
        } else if (value instanceof Long l) {
            ps.setLong(index, l);
        } else if (value instanceof Double d) {
            ps.setDouble(index, d);
        } else if (value instanceof Float f) {
            ps.setFloat(index, f);
        } else if (value instanceof Boolean b) {
            ps.setBoolean(index, b);
        } else if (value instanceof java.sql.Date date) {
            ps.setDate(index, date);
        } else if (value instanceof java.sql.Time time) {
            ps.setTime(index, time);
        } else if (value instanceof Timestamp ts) {
            ps.setTimestamp(index, ts);
        } else if (value instanceof byte[] bytes) {
            ps.setBytes(index, bytes);
        } else {
            ps.setObject(index, value);
        }
    }

    private int countInserted(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0 || result == Statement.SUCCESS_NO_INFO) {
                count++;
            }
        }
        return count;
    }

    private void cacheGeneratedKeys(PreparedStatement ps, TableMetadata table, ColumnMetadata pkColumn)
            throws SQLException {
        try (ResultSet keys = ps.getGeneratedKeys()) {
            while (keys.next()) {
                Object generatedKey = keys.getObject(1);
                dataGenerator.updateForeignKeyCache(table.tableName(), pkColumn.name(), generatedKey);
            }
        }
    }

    public List<Object> getExistingIds(String tableName, String columnName, int limit) throws SQLException {
        List<Object> ids = new ArrayList<>();
        String sql = String.format("SELECT %s FROM %s LIMIT ?", columnName, tableName);

        log.debug("Fetching existing IDs from {}.{} (limit: {})", tableName, columnName, limit);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getObject(1));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to fetch existing IDs from {}.{}", tableName, columnName, e);
            throw e;
        }

        log.debug("Found {} existing IDs in {}.{}", ids.size(), tableName, columnName);
        return ids;
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int inserted, int total);
    }
}
