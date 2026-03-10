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
public class UserInfoInsertService {

    private static final Logger log = LoggerFactory.getLogger(UserInfoInsertService.class);
    private static final int BATCH_SIZE = 1000;

    private final DataSource dataSource;
    private final SchemaDetectionService schemaService;
    private final DataGeneratorService dataGenerator;

    public UserInfoInsertService(DataSource dataSource,
                                 SchemaDetectionService schemaService,
                                 DataGeneratorService dataGenerator) {
        this.dataSource = dataSource;
        this.schemaService = schemaService;
        this.dataGenerator = dataGenerator;
    }

    /**
     * Inserts roles, groups, users, and membership mappings in one transaction flow.
     * Returns total rows inserted across all tables.
     *
     * Tables written (in order):
     *   userrole, userroleauthorities, usergroup, userinfo, userrolemembers, usergroupmembers
     */
    public record UserInsertResult(int totalInserted, long firstUserId, int userCount) {}

    public UserInsertResult insertUsers(int amount, int amountRoles, int amountUserGroups,
                                        ProgressCallback callback) throws SQLException {
        log.info("Starting userinfo insert: {} users, {} roles, {} groups", amount, amountRoles, amountUserGroups);

        TableMetadata roleTable = schemaService.getTable("userrole");
        TableMetadata groupTable = schemaService.getTable("usergroup");
        TableMetadata userTable = schemaService.getTable("userinfo");

        if (roleTable == null) throw new IllegalArgumentException("Table not found: userrole");
        if (groupTable == null) throw new IllegalArgumentException("Table not found: usergroup");
        if (userTable == null) throw new IllegalArgumentException("Table not found: userinfo");

        int total = 0;
        long capturedFirstUserId = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // --- Phase 1: roles (userrole + userroleauthorities) ---
                long firstRoleId = getMaxPk(conn, "userrole", "userroleid") + 1;
                log.info("Inserting {} roles starting at userroleid={}", amountRoles, firstRoleId);
                total += insertRoles(conn, firstRoleId, amountRoles, roleTable);
                if (callback != null) callback.onProgress(total);

                // --- Phase 2: groups (usergroup) ---
                long firstGroupId = getMaxPk(conn, "usergroup", "usergroupid") + 1;
                log.info("Inserting {} groups starting at usergroupid={}", amountUserGroups, firstGroupId);
                total += insertGroups(conn, firstGroupId, amountUserGroups, groupTable);
                if (callback != null) callback.onProgress(total);

                // --- Phase 3: users (userinfo) ---
                capturedFirstUserId = getMaxPk(conn, "userinfo", "userinfoid") + 1;
                log.info("Inserting {} users starting at userinfoid={}", amount, capturedFirstUserId);
                total += insertUserInfoRows(conn, capturedFirstUserId, amount, userTable, callback, total);

                // --- Phase 4: role memberships (userrolemembers) ---
                log.info("Inserting {} userrolemembers rows", amount);
                total += insertRoleMemberships(conn, capturedFirstUserId, amount, firstRoleId, amountRoles, callback, total);

                // --- Phase 5: group memberships (usergroupmembers) ---
                log.info("Inserting {} usergroupmembers rows", amount);
                total += insertGroupMemberships(conn, capturedFirstUserId, amount, firstGroupId, amountUserGroups, callback, total);

                conn.commit();

            } catch (SQLException e) {
                conn.rollback();
                log.error("UserInfo insert failed, rolling back all changes", e);
                throw e;
            }
        }

        log.info("UserInfo insert complete: {} total rows inserted", total);
        return new UserInsertResult(total, capturedFirstUserId, amount);
    }

    private int insertRoles(Connection conn, long firstRoleId, int count,
                             TableMetadata table) throws SQLException {
        List<ColumnMetadata> columns = table.getInsertableColumns();
        String roleSql = buildInsertSql("userrole", columns);
        String authSql = "INSERT INTO userroleauthorities (userroleid, authority) VALUES (?, ?)";
        int inserted = 0;

        try (PreparedStatement rolePs = conn.prepareStatement(roleSql);
             PreparedStatement authPs = conn.prepareStatement(authSql)) {

            for (int i = 0; i < count; i++) {
                long id = firstRoleId + i;
                String name = "Role-" + dataGenerator.generateRandomSuffix();

                Map<String, Object> row = dataGenerator.generateRow(table);
                row.put("userroleid", id);
                row.put("name", name);

                setParameters(rolePs, columns, row);
                rolePs.addBatch();

                authPs.setLong(1, id);
                authPs.setString(2, name);
                authPs.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    rolePs.executeBatch();
                    rolePs.clearBatch();
                    authPs.executeBatch();
                    authPs.clearBatch();
                    conn.commit();
                    inserted = i + 1;
                }
            }

            if (count % BATCH_SIZE != 0) {
                rolePs.executeBatch();
                authPs.executeBatch();
                conn.commit();
                inserted = count;
            }
        }

        return inserted * 2; // userrole + userroleauthorities
    }

    private int insertGroups(Connection conn, long firstGroupId, int count,
                              TableMetadata table) throws SQLException {
        List<ColumnMetadata> columns = table.getInsertableColumns();
        String sql = buildInsertSql("usergroup", columns);
        int inserted = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);
                row.put("usergroupid", firstGroupId + i);
                row.put("name", "Group-" + dataGenerator.generateRandomSuffix());

                setParameters(ps, columns, row);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                    conn.commit();
                    inserted = i + 1;
                }
            }

            if (count % BATCH_SIZE != 0) {
                ps.executeBatch();
                conn.commit();
                inserted = count;
            }
        }

        return inserted;
    }

    private int insertUserInfoRows(Connection conn, long firstUserId, int amount,
                                   TableMetadata table, ProgressCallback callback,
                                   int runningTotal) throws SQLException {
        List<ColumnMetadata> columns = table.getInsertableColumns();
        String sql = buildInsertSql("userinfo", columns);
        int inserted = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < amount; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);
                row.put("userinfoid", firstUserId + i);
                row.put("twofactortype", "NOT_ENABLED");

                setParameters(ps, columns, row);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                    conn.commit();
                    inserted = i + 1;
                    if (callback != null) callback.onProgress(runningTotal + inserted);
                }
            }

            if (amount % BATCH_SIZE != 0) {
                ps.executeBatch();
                conn.commit();
                inserted = amount;
                if (callback != null) callback.onProgress(runningTotal + inserted);
            }
        }

        return inserted;
    }

    private int insertRoleMemberships(Connection conn, long firstUserId, int amount,
                                      long firstRoleId, int amountRoles,
                                      ProgressCallback callback, int runningTotal) throws SQLException {
        String sql = "INSERT INTO userrolemembers (userroleid, userid) VALUES (?, ?)";
        int inserted = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < amount; i++) {
                ps.setLong(1, firstRoleId + (i % amountRoles));
                ps.setLong(2, firstUserId + i);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                    conn.commit();
                    inserted = i + 1;
                    if (callback != null) callback.onProgress(runningTotal + inserted);
                }
            }

            if (amount % BATCH_SIZE != 0) {
                ps.executeBatch();
                conn.commit();
                inserted = amount;
                if (callback != null) callback.onProgress(runningTotal + inserted);
            }
        }

        return inserted;
    }

    private int insertGroupMemberships(Connection conn, long firstUserId, int amount,
                                       long firstGroupId, int amountUserGroups,
                                       ProgressCallback callback, int runningTotal) throws SQLException {
        String sql = "INSERT INTO usergroupmembers (usergroupid, userid) VALUES (?, ?)";
        int inserted = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < amount; i++) {
                ps.setLong(1, firstGroupId + (i % amountUserGroups));
                ps.setLong(2, firstUserId + i);
                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                    conn.commit();
                    inserted = i + 1;
                    if (callback != null) callback.onProgress(runningTotal + inserted);
                }
            }

            if (amount % BATCH_SIZE != 0) {
                ps.executeBatch();
                conn.commit();
                inserted = amount;
                if (callback != null) callback.onProgress(runningTotal + inserted);
            }
        }

        return inserted;
    }

    private long getMaxPk(Connection conn, String table, String pkColumn) throws SQLException {
        String sql = "SELECT COALESCE(MAX(" + pkColumn + "), 0) FROM " + table;
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
