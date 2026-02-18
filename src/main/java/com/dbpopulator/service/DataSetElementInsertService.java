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
public class DataSetElementInsertService {

    private static final Logger log = LoggerFactory.getLogger(DataSetElementInsertService.class);
    private static final int BATCH_SIZE = 1000;
    private static final Set<String> DATASET_EXTRA_COLUMNS = Set.of("version", "expirydays", "timelydays", "openfutureperiods");

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    public DataSetElementInsertService(DataSource dataSource,
                                        DataGeneratorService dataGenerator,
                                        SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public int insertDataSetElements(int amount, List<Long> categoryComboIds,
                                      ProgressCallback callback) throws SQLException {
        log.info("Starting datasetelement insert: {} dataelements + {} datasets + {} join rows, {} categorycomboid values",
            amount, amount, amount, categoryComboIds.size());

        if (categoryComboIds.isEmpty()) {
            throw new IllegalArgumentException("categoryComboIds array cannot be empty");
        }

        int totalInserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Step 1: Query a default periodtypeid from the periodtype table
                long periodTypeId = queryDefaultPeriodTypeId(conn);
                log.info("Using default periodtypeid: {}", periodTypeId);

                // Step 2: Insert dataelements
                log.info("Inserting {} dataelements", amount);
                List<Long> dataElementIds = insertEntities(conn, "dataelement", amount, row -> {
                    long selectedComboId = categoryComboIds.get(
                        ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                    row.put("categorycomboid", selectedComboId);
                    row.put("valuetype", "TEXT");
                    row.put("domaintype", "AGGREGATE");
                    row.put("aggregationtype", "DEFAULT");
                }, false);
                totalInserted += dataElementIds.size();
                if (callback != null) {
                    callback.onProgress(totalInserted);
                }
                log.info("Inserted {} dataelements", dataElementIds.size());

                // Step 3: Insert datasets
                log.info("Inserting {} datasets", amount);
                List<Long> dataSetIds = insertEntities(conn, "dataset", amount, row -> {
                    long selectedComboId = categoryComboIds.get(
                        ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                    row.put("categorycomboid", selectedComboId);
                    row.put("periodtypeid", periodTypeId);
                    row.put("version", 1);
                    row.put("expirydays", 0);
                    row.put("timelydays", 0);
                    row.put("openfutureperiods", 0);
                }, true);
                totalInserted += dataSetIds.size();
                if (callback != null) {
                    callback.onProgress(totalInserted);
                }
                log.info("Inserted {} datasets", dataSetIds.size());

                conn.commit();

                // Step 4: Insert datasetelement join table entries (1:1 mapping)
                log.info("Inserting {} datasetelement join entries", amount);
                int joinCount = insertDataSetElementJoinEntries(dataElementIds, dataSetIds);
                totalInserted += joinCount;
                if (callback != null) {
                    callback.onProgress(totalInserted);
                }
                log.info("Inserted {} datasetelement join entries", joinCount);

            } catch (SQLException e) {
                conn.rollback();
                log.error("DataSetElement insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("DataSetElement insert complete: {} total rows", totalInserted);
        return totalInserted;
    }

    private long queryDefaultPeriodTypeId(Connection conn) throws SQLException {
        String sql = "SELECT periodtypeid FROM periodtype LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new IllegalArgumentException("No rows found in periodtype table");
        }
    }

    private List<Long> insertEntities(Connection conn, String tableName, int count,
                                       RowOverride override, boolean useExtraColumns) throws SQLException {
        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        ColumnMetadata pk = table.getPrimaryKeyColumn();
        boolean selfGeneratedPk = pk != null && !pk.isAutoIncrement();

        List<ColumnMetadata> insertableColumns = useExtraColumns
            ? getInsertableColumnsWithExtras(table)
            : table.getInsertableColumns();
        String sql = buildInsertSql(tableName, insertableColumns);
        List<Long> insertedIds = new ArrayList<>();
        List<Long> batchPkValues = selfGeneratedPk ? new ArrayList<>() : null;

        try (PreparedStatement ps = selfGeneratedPk
                ? conn.prepareStatement(sql)
                : conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);

                // Apply overrides
                override.apply(row);

                // Capture self-generated PK value before inserting
                if (selfGeneratedPk && pk != null) {
                    Object pkValue = row.get(pk.name());
                    if (pkValue instanceof Number num) {
                        batchPkValues.add(num.longValue());
                    }
                }

                setParameters(ps, insertableColumns, row);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0 || i == count - 1) {
                    ps.executeBatch();

                    if (selfGeneratedPk) {
                        insertedIds.addAll(batchPkValues);
                        batchPkValues.clear();
                    } else {
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            while (keys.next()) {
                                insertedIds.add(keys.getLong(1));
                            }
                        }
                    }
                    ps.clearBatch();
                }
            }
        }

        // Update FK cache for the inserted entities
        if (pk != null) {
            for (Long id : insertedIds) {
                dataGenerator.updateForeignKeyCache(tableName, pk.name(), id);
            }
        }

        return insertedIds;
    }

    private int insertDataSetElementJoinEntries(List<Long> dataElementIds, List<Long> dataSetIds) throws SQLException {
        TableMetadata joinTable = schemaService.getTable("datasetelement");
        if (joinTable == null) {
            throw new IllegalArgumentException("Join table not found: datasetelement");
        }

        // Find FK columns dynamically by inspecting referencedTable
        String dataElementFkCol = null;
        String dataSetFkCol = null;

        for (ColumnMetadata col : joinTable.columns()) {
            if (col.isForeignKey()) {
                if ("dataelement".equalsIgnoreCase(col.referencedTable())) {
                    dataElementFkCol = col.name();
                } else if ("dataset".equalsIgnoreCase(col.referencedTable())) {
                    dataSetFkCol = col.name();
                }
            }
        }

        if (dataElementFkCol == null || dataSetFkCol == null) {
            throw new IllegalArgumentException("Cannot determine FK columns for datasetelement join table");
        }

        String sql = String.format("INSERT INTO datasetelement (%s, %s) VALUES (?, ?)",
            dataSetFkCol, dataElementFkCol);

        int inserted = 0;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < dataElementIds.size(); i++) {
                    ps.setLong(1, dataSetIds.get(i));
                    ps.setLong(2, dataElementIds.get(i));
                    ps.addBatch();
                    inserted++;

                    if (inserted % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }

                if (inserted % BATCH_SIZE != 0) {
                    ps.executeBatch();
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }

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
            if (DATASET_EXTRA_COLUMNS.contains(col.name().toLowerCase()) && !alreadyIncluded.contains(col.name().toLowerCase())) {
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
    private interface RowOverride {
        void apply(Map<String, Object> row);
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
