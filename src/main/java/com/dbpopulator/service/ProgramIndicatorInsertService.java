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
public class ProgramIndicatorInsertService {

    private static final Logger log = LoggerFactory.getLogger(ProgramIndicatorInsertService.class);
    private static final int BATCH_SIZE = 1000;
    private static final int PROGRAM_COUNT = 10;

    // Nullable columns in programindicator that must be forced into the insert
    private static final Set<String> INDICATOR_EXTRA_COLUMNS = Set.of(
        "programid", "analyticstype", "categorycomboid", "attributecomboid"
    );

    // Nullable primitive columns in program that need explicit defaults
    private static final Set<String> PROGRAM_EXTRA_COLUMNS = Set.of(
        "version", "maxteicounttoreturn", "minattributesrequiredtosearch",
        "expirydays", "completeeventsexpirydays", "opendaysaftercoitheo",
        "enrollmentdatelabel", "incidentdatelabel", "enablechangelog", "programtype", "type"
    );

    private static final Map<String, Object> PROGRAM_PRIMITIVE_DEFAULTS = Map.ofEntries(
        Map.entry("version", 1),
        Map.entry("maxteicounttoreturn", 0),
        Map.entry("minattributesrequiredtosearch", 1),
        Map.entry("expirydays", 0),
        Map.entry("completeeventsexpirydays", 0),
        Map.entry("opendaysaftercoitheo", 0),
        Map.entry("enablechangelog", false),
        Map.entry("programtype", "WITHOUT_REGISTRATION"),
        Map.entry("type", "WITHOUT_REGISTRATION")
    );

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    public ProgramIndicatorInsertService(DataSource dataSource,
                                          DataGeneratorService dataGenerator,
                                          SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public int insertProgramIndicators(int amount, List<Long> categoryComboIds,
                                        ProgressCallback callback) throws SQLException {
        log.info("Starting program indicator insert: {} rows, {} categoryComboId values",
            amount, categoryComboIds.size());

        TableMetadata programTable = schemaService.getTable("program");
        if (programTable == null) {
            throw new IllegalArgumentException("Table not found: program");
        }

        TableMetadata indicatorTable = schemaService.getTable("programindicator");
        if (indicatorTable == null) {
            throw new IllegalArgumentException("Table not found: programindicator");
        }

        int totalInserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Step 1: create programs and capture their IDs for the FK
            List<Long> programIds = createPrograms(conn, categoryComboIds, programTable);
            totalInserted += programIds.size();
            if (callback != null) {
                callback.onProgress(totalInserted);
            }

            // Step 2: insert programindicator rows
            // Use extras to force nullable columns (programid, analyticstype, etc.) into the SQL
            List<ColumnMetadata> columns = getIndicatorInsertableColumnsWithExtras(indicatorTable);
            String sql = buildInsertSql("programindicator", columns);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < amount; i++) {
                    Map<String, Object> row = dataGenerator.generateRow(indicatorTable);

                    // Override FK and enum columns
                    long programId = programIds.get(ThreadLocalRandom.current().nextInt(programIds.size()));
                    row.put("programid", programId);

                    long comboId = categoryComboIds.get(ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                    row.put("categorycomboid", comboId);

                    long attrComboId = categoryComboIds.get(ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                    row.put("attributecomboid", attrComboId);

                    row.put("analyticstype", "EVENT");

                    // Safe defaults for any remaining null primitive columns
                    for (ColumnMetadata col : columns) {
                        if (row.get(col.name()) == null) {
                            switch (col.sqlType()) {
                                case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> row.put(col.name(), 0);
                                case Types.BIGINT -> row.put(col.name(), 0L);
                                case Types.BOOLEAN, Types.BIT -> row.put(col.name(), false);
                            }
                        }
                    }

                    setParameters(ps, columns, row);
                    ps.addBatch();

                    if ((i + 1) % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                        conn.commit();
                        totalInserted = programIds.size() + i + 1;
                        if (callback != null) {
                            callback.onProgress(totalInserted);
                        }
                        log.debug("Committed batch, {} total rows inserted so far", totalInserted);
                    }
                }

                if (amount % BATCH_SIZE != 0) {
                    ps.executeBatch();
                    conn.commit();
                    totalInserted = programIds.size() + amount;
                    if (callback != null) {
                        callback.onProgress(totalInserted);
                    }
                }

            } catch (SQLException e) {
                conn.rollback();
                log.error("Program indicator insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("Program indicator insert complete: {} programs + {} indicators = {} total",
            PROGRAM_COUNT, amount, totalInserted);
        return totalInserted;
    }

    private List<Long> createPrograms(Connection conn, List<Long> categoryComboIds,
                                       TableMetadata programTable) throws SQLException {
        List<ColumnMetadata> columns = getProgramInsertableColumnsWithExtras(programTable);
        Set<String> columnNames = new HashSet<>();
        for (ColumnMetadata col : columns) {
            columnNames.add(col.name().toLowerCase());
        }

        StringBuilder sql = new StringBuilder("INSERT INTO program (");
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
                placeholders.append(", ");
            }
            sql.append(columns.get(i).name());
            placeholders.append("?");
        }
        sql.append(") VALUES (").append(placeholders).append(") RETURNING programid");

        List<Long> ids = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < PROGRAM_COUNT; i++) {
                Map<String, Object> row = dataGenerator.generateRow(programTable);

                long comboId = categoryComboIds.get(ThreadLocalRandom.current().nextInt(categoryComboIds.size()));
                row.put("categorycomboid", comboId);
                row.put("enrollmentcategorycomboid", categoryComboIds.get(
                    ThreadLocalRandom.current().nextInt(categoryComboIds.size())));
                row.put("programtype", "WITHOUT_REGISTRATION");

                for (Map.Entry<String, Object> entry : PROGRAM_PRIMITIVE_DEFAULTS.entrySet()) {
                    if (columnNames.contains(entry.getKey())) {
                        row.putIfAbsent(entry.getKey(), entry.getValue());
                    }
                }

                for (ColumnMetadata col : columns) {
                    if (row.get(col.name()) == null) {
                        switch (col.sqlType()) {
                            case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> row.put(col.name(), 0);
                            case Types.BIGINT -> row.put(col.name(), 0L);
                            case Types.BOOLEAN, Types.BIT -> row.put(col.name(), false);
                        }
                    }
                }

                setParameters(ps, columns, row);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        ids.add(rs.getLong(1));
                    }
                }
            }
        }

        conn.commit();
        log.info("Created {} programs for programindicator FK dependency", ids.size());
        return ids;
    }

    private List<ColumnMetadata> getIndicatorInsertableColumnsWithExtras(TableMetadata table) {
        List<ColumnMetadata> base = table.getInsertableColumns();
        Set<String> alreadyIncluded = new HashSet<>();
        for (ColumnMetadata col : base) {
            alreadyIncluded.add(col.name().toLowerCase());
        }

        List<ColumnMetadata> result = new ArrayList<>(base);
        for (ColumnMetadata col : table.columns()) {
            if (INDICATOR_EXTRA_COLUMNS.contains(col.name().toLowerCase())
                    && !alreadyIncluded.contains(col.name().toLowerCase())) {
                result.add(col);
            }
        }
        return result;
    }

    private List<ColumnMetadata> getProgramInsertableColumnsWithExtras(TableMetadata table) {
        List<ColumnMetadata> base = table.getInsertableColumns();
        Set<String> alreadyIncluded = new HashSet<>();
        for (ColumnMetadata col : base) {
            alreadyIncluded.add(col.name().toLowerCase());
        }

        List<ColumnMetadata> result = new ArrayList<>(base);
        for (ColumnMetadata col : table.columns()) {
            if (PROGRAM_EXTRA_COLUMNS.contains(col.name().toLowerCase())
                    && !alreadyIncluded.contains(col.name().toLowerCase())) {
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
            if (value == null && ("programtype".equalsIgnoreCase(col.name()) || "type".equalsIgnoreCase(col.name()))) {
                value = "WITHOUT_REGISTRATION";
            }
            if (value == null && "enablechangelog".equalsIgnoreCase(col.name())) {
                value = false;
            }
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
