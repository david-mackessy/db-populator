package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.PopulateRequest.UserRoleEntry;
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
public class UserRoleInsertService {

    private static final Logger log = LoggerFactory.getLogger(UserRoleInsertService.class);

    private final DataSource dataSource;
    private final SchemaDetectionService schemaService;
    private final DataGeneratorService dataGenerator;

    public UserRoleInsertService(DataSource dataSource,
                                 SchemaDetectionService schemaService,
                                 DataGeneratorService dataGenerator) {
        this.dataSource = dataSource;
        this.schemaService = schemaService;
        this.dataGenerator = dataGenerator;
    }

    public int insertUserRoles(List<UserRoleEntry> entries, ProgressCallback callback) throws SQLException {
        log.info("Starting userrole insert: {} role-authority pairs", entries.size());

        TableMetadata table = schemaService.getTable("userrole");
        if (table == null) {
            throw new IllegalArgumentException("Table not found: userrole");
        }
        List<ColumnMetadata> columns = table.getInsertableColumns();
        String roleSql = buildInsertSql("userrole", columns);
        String authSql = "INSERT INTO userroleauthorities (userroleid, authority) VALUES (?, ?)";

        int total = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                long nextId = getMaxPkValue(conn) + 1;
                log.info("Starting userroleid from {}", nextId);

                for (int i = 0; i < entries.size(); i++) {
                    UserRoleEntry entry = entries.get(i);
                    long userRoleId = nextId + i;

                    Map<String, Object> row = dataGenerator.generateRow(table);
                    row.put("userroleid", userRoleId);
                    row.put("name", entry.role());

                    try (PreparedStatement rolePs = conn.prepareStatement(roleSql);
                         PreparedStatement authPs = conn.prepareStatement(authSql)) {
                        setParameters(rolePs, columns, row);
                        rolePs.executeUpdate();

                        authPs.setLong(1, userRoleId);
                        authPs.setString(2, entry.authority());
                        authPs.executeUpdate();
                    }

                    total += 2;
                    conn.commit();

                    if (callback != null) callback.onProgress(total);
                    log.debug("Inserted userrole '{}' (id={}) with authority '{}'",
                        entry.role(), userRoleId, entry.authority());
                }
            } catch (SQLException e) {
                conn.rollback();
                log.error("UserRole insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("UserRole insert complete: {} total rows ({} roles + {} authorities)",
            total, entries.size(), entries.size());
        return total;
    }

    private long getMaxPkValue(Connection conn) throws SQLException {
        String sql = "SELECT COALESCE(MAX(userroleid), 0) FROM userrole";
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

    private String generateUid() {
        String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lower = "abcdefghijklmnopqrstuvwxyz";
        String digits = "0123456789";
        String all = upper + lower + digits;
        StringBuilder sb = new StringBuilder(11);
        sb.append(upper.charAt(ThreadLocalRandom.current().nextInt(upper.length())));
        int lowerPos = ThreadLocalRandom.current().nextInt(1, 11);
        int digitPos;
        do { digitPos = ThreadLocalRandom.current().nextInt(1, 11); } while (digitPos == lowerPos);
        for (int i = 1; i < 11; i++) {
            if (i == lowerPos) sb.append(lower.charAt(ThreadLocalRandom.current().nextInt(lower.length())));
            else if (i == digitPos) sb.append(digits.charAt(ThreadLocalRandom.current().nextInt(digits.length())));
            else sb.append(all.charAt(ThreadLocalRandom.current().nextInt(all.length())));
        }
        return sb.toString();
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
