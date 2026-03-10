package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.TableMetadata;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class UserGroupInsertService {

    private static final Logger log = LoggerFactory.getLogger(UserGroupInsertService.class);
    private static final int BATCH_SIZE = 1000;

    private final DataSource dataSource;
    private final SchemaDetectionService schemaService;
    private final DataGeneratorService dataGenerator;

    public UserGroupInsertService(DataSource dataSource,
                                  SchemaDetectionService schemaService,
                                  DataGeneratorService dataGenerator) {
        this.dataSource = dataSource;
        this.schemaService = schemaService;
        this.dataGenerator = dataGenerator;
    }

    public int insertUserGroups(int amount, ProgressCallback callback) throws SQLException {
        log.info("Starting usergroup insert: {} rows", amount);

        TableMetadata table = schemaService.getTable("usergroup");
        if (table == null) {
            throw new IllegalArgumentException("Table not found: usergroup");
        }
        List<ColumnMetadata> columns = table.getInsertableColumns();
        String sql = buildInsertSql("usergroup", columns);

        int inserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                long nextId = getMaxPkValue(conn) + 1;
                log.info("Starting usergroupid from {}", nextId);

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (int i = 0; i < amount; i++) {
                        Map<String, Object> row = dataGenerator.generateRow(table);
                        row.put("usergroupid", nextId + i);
                        row.put("name", "Group-" + dataGenerator.generateRandomSuffix());

                        setParameters(ps, columns, row);
                        ps.addBatch();

                        if ((i + 1) % BATCH_SIZE == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                            conn.commit();
                            inserted = i + 1;
                            if (callback != null) callback.onProgress(inserted);
                            log.debug("Committed batch, {} rows inserted so far", inserted);
                        }
                    }

                    if (amount % BATCH_SIZE != 0) {
                        ps.executeBatch();
                        conn.commit();
                        inserted = amount;
                        if (callback != null) callback.onProgress(inserted);
                    }
                }

            } catch (SQLException e) {
                conn.rollback();
                log.error("UserGroup insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("UserGroup insert complete: {} rows inserted", inserted);
        return inserted;
    }

    private long getMaxPkValue(Connection conn) throws SQLException {
        String sql = "SELECT COALESCE(MAX(usergroupid), 0) FROM usergroup";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
            return 0;
        }
    }

    private String buildInsertSql(String tableName, List<ColumnMetadata> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) { sql.append(", "); placeholders.append(", "); }
            sql.append(columns.get(i).name());
            placeholders.append("?");
        }
        return sql.append(") VALUES (").append(placeholders).append(")").toString();
    }

    private void setParameters(PreparedStatement ps, List<ColumnMetadata> columns,
                                Map<String, Object> row) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            setParameter(ps, i + 1, row.get(col.name()), col.sqlType());
        }
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int sqlType) throws SQLException {
        if (value == null) { ps.setNull(index, sqlType); return; }
        if (value instanceof java.util.UUID uuid) ps.setObject(index, uuid);
        else if (value instanceof com.dbpopulator.model.JsonValue json) {
            PGobject pg = new PGobject(); pg.setType("jsonb"); pg.setValue(json.value()); ps.setObject(index, pg);
        }
        else if (value instanceof String s) ps.setString(index, s);
        else if (value instanceof Integer i) ps.setInt(index, i);
        else if (value instanceof Long l) ps.setLong(index, l);
        else if (value instanceof Double d) ps.setDouble(index, d);
        else if (value instanceof Float f) ps.setFloat(index, f);
        else if (value instanceof Boolean b) ps.setBoolean(index, b);
        else if (value instanceof java.sql.Date date) ps.setDate(index, date);
        else if (value instanceof java.sql.Time time) ps.setTime(index, time);
        else if (value instanceof Timestamp ts) ps.setTimestamp(index, ts);
        else if (value instanceof byte[] bytes) ps.setBytes(index, bytes);
        else ps.setObject(index, value);
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
