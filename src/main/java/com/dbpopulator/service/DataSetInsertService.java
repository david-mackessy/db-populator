package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.TableMetadata;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class DataSetInsertService {

    private static final Logger log = LoggerFactory.getLogger(DataSetInsertService.class);
    private static final int BATCH_SIZE = 1000;
    private static final Set<String> EXTRA_COLUMNS = Set.of("version", "expirydays", "timelydays", "openfutureperiods");

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    public DataSetInsertService(DataSource dataSource,
                                 DataGeneratorService dataGenerator,
                                 SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public int insertDataSets(int amount, List<Long> categoryComboIds, List<Long> periodTypeIds,
                               ProgressCallback callback) throws SQLException {
        log.info("Starting dataset insert: {} rows with {} categorycomboid values, {} periodTypeIds",
            amount, categoryComboIds.size(), periodTypeIds.size());

        if (categoryComboIds.isEmpty()) {
            throw new IllegalArgumentException("categoryComboIds array cannot be empty");
        }

        TableMetadata table = schemaService.getTable("dataset");
        if (table == null) {
            throw new IllegalArgumentException("Table not found: dataset");
        }

        List<ColumnMetadata> insertableColumns = getInsertableColumnsWithExtras(table);
        String sql = buildInsertSql("dataset", insertableColumns);

        int inserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < amount; i++) {
                    Map<String, Object> row = dataGenerator.generateRow(table);

                    // Override columns with request-provided values
                    long selectedComboId = categoryComboIds.get(
                        ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                    long selectedPeriodTypeId = periodTypeIds.get(
                        ThreadLocalRandom.current().nextInt(periodTypeIds.size()));
                    row.put("categorycomboid", selectedComboId);
                    row.put("periodtypeid", selectedPeriodTypeId);

                    // Default values for optional primitive columns
                    row.put("version", 1);
                    row.put("expirydays", 0);
                    row.put("timelydays", 0);
                    row.put("openfutureperiods", 0);

                    setParameters(ps, insertableColumns, row);
                    ps.addBatch();

                    if ((i + 1) % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                        conn.commit();
                        inserted = i + 1;
                        if (callback != null) {
                            callback.onProgress(inserted);
                        }
                        log.debug("Committed batch, {} rows inserted so far", inserted);
                    }
                }

                // Execute remaining batch
                if (amount % BATCH_SIZE != 0) {
                    ps.executeBatch();
                    conn.commit();
                    inserted = amount;
                    if (callback != null) {
                        callback.onProgress(inserted);
                    }
                }

            } catch (SQLException e) {
                conn.rollback();
                log.error("Dataset insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("Dataset insert complete: {} rows inserted", inserted);
        return inserted;
    }

    private List<ColumnMetadata> getInsertableColumnsWithExtras(TableMetadata table) {
        List<ColumnMetadata> base = table.getInsertableColumns();
        Set<String> alreadyIncluded = new HashSet<>();
        for (ColumnMetadata col : base) {
            alreadyIncluded.add(col.name().toLowerCase());
        }

        List<ColumnMetadata> result = new ArrayList<>(base);
        for (ColumnMetadata col : table.columns()) {
            if (EXTRA_COLUMNS.contains(col.name().toLowerCase()) && !alreadyIncluded.contains(col.name().toLowerCase())) {
                result.add(col);
            }
        }
        return result;
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

    private void setParameters(PreparedStatement ps, List<ColumnMetadata> columns,
                                Map<String, Object> row) throws SQLException {
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

        if (value instanceof java.util.UUID uuid) {
            ps.setObject(index, uuid);
        } else if (value instanceof com.dbpopulator.model.JsonValue json) {
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

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
