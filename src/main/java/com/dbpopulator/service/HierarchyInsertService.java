package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.TableMetadata;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HierarchyInsertService {

    private static final Logger log = LoggerFactory.getLogger(HierarchyInsertService.class);

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    public HierarchyInsertService(DataSource dataSource,
                                   DataGeneratorService dataGenerator,
                                   SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public HierarchyResult insertHierarchy(String tableName, String parentColumn, List<Integer> hierarchy,
                                            Long orgunitgroupid, ProgressCallback callback) throws SQLException {
        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        ColumnMetadata pkColumn = table.getPrimaryKeyColumn();
        if (pkColumn == null) {
            throw new IllegalArgumentException("Table has no primary key: " + tableName);
        }

        List<ColumnMetadata> insertableColumns = table.getInsertableColumns();
        // Add parent column if it's not already in insertable columns (it might be nullable)
        ColumnMetadata parentCol = table.columns().stream()
            .filter(c -> c.name().equalsIgnoreCase(parentColumn))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Parent column not found: " + parentColumn));

        boolean parentInInsertable = insertableColumns.stream()
            .anyMatch(c -> c.name().equalsIgnoreCase(parentColumn));

        List<ColumnMetadata> columnsToInsert = new ArrayList<>(insertableColumns);
        if (!parentInInsertable) {
            columnsToInsert.add(parentCol);
        }

        // For organisationunit table, ensure path, hierarchylevel, and openingdate columns are included
        boolean isOrgUnit = tableName.equalsIgnoreCase("organisationunit");
        if (isOrgUnit) {
            addColumnIfMissing(columnsToInsert, table, "path");
            addColumnIfMissing(columnsToInsert, table, "hierarchylevel");
            addColumnIfMissing(columnsToInsert, table, "openingdate");
        }

        String sql = buildInsertSql(tableName, columnsToInsert, pkColumn.name());
        log.debug("Hierarchy insert SQL: {}", sql);

        int totalInserted = 0;
        List<Object> currentLevelIds = new ArrayList<>();
        List<Object> allInsertedIds = new ArrayList<>();
        // Track id -> path mapping for organisationunit
        Map<Object, String> idToPath = new HashMap<>();
        // Track ids per level for auto org unit group assignment
        List<List<Object>> orgUnitsByLevel = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                for (int level = 0; level < hierarchy.size(); level++) {
                    int countPerParent = hierarchy.get(level);
                    List<Object> parentIds = new ArrayList<>(currentLevelIds);
                    currentLevelIds.clear();
                    int hierarchyLevel = level + 1;

                    if (level == 0) {
                        // Root level - no parents
                        log.info("Inserting {} root nodes at level {}", countPerParent, level);
                        List<InsertedRow> newRows = insertLevel(conn, sql, table, columnsToInsert,
                            pkColumn, parentColumn, null, null, hierarchyLevel, isOrgUnit, countPerParent);
                        for (InsertedRow row : newRows) {
                            currentLevelIds.add(row.id());
                            allInsertedIds.add(row.id());
                            if (isOrgUnit) {
                                idToPath.put(row.id(), row.path());
                            }
                        }
                        totalInserted += newRows.size();
                    } else {
                        // Child levels - insert for each parent
                        int levelTotal = parentIds.size() * countPerParent;
                        log.info("Inserting {} nodes at level {} ({} parents × {} children)",
                            levelTotal, level, parentIds.size(), countPerParent);

                        for (Object parentId : parentIds) {
                            String parentPath = isOrgUnit ? idToPath.get(parentId) : null;
                            List<InsertedRow> newRows = insertLevel(conn, sql, table, columnsToInsert,
                                pkColumn, parentColumn, parentId, parentPath, hierarchyLevel, isOrgUnit, countPerParent);
                            for (InsertedRow row : newRows) {
                                currentLevelIds.add(row.id());
                                allInsertedIds.add(row.id());
                                if (isOrgUnit) {
                                    idToPath.put(row.id(), row.path());
                                }
                            }
                            totalInserted += newRows.size();

                            if (callback != null) {
                                callback.onProgress(totalInserted);
                            }
                        }
                    }

                    conn.commit();
                    log.info("Level {} complete: {} total inserted so far", level, totalInserted);

                    if (isOrgUnit) {
                        orgUnitsByLevel.add(new ArrayList<>(currentLevelIds));
                    }

                    if (callback != null) {
                        callback.onProgress(totalInserted);
                    }
                }

            } catch (SQLException e) {
                conn.rollback();
                log.error("Hierarchy insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("Hierarchy insert complete: {} total rows", totalInserted);

        if (isOrgUnit && !allInsertedIds.isEmpty()) {
            if (orgunitgroupid != null) {
                // Legacy: assign all org units to the single provided group
                log.debug("Assigning all {} org units to provided group {}", allInsertedIds.size(), orgunitgroupid);
                insertOrgUnitGroupMemberships(orgunitgroupid, allInsertedIds);
            } else {
                // Auto: create one org unit group per hierarchy level and assign accordingly
                log.info("Auto-creating {} org unit groups (one per hierarchy level)", orgUnitsByLevel.size());
                insertAutoOrgUnitGroupsPerLevel(orgUnitsByLevel);
            }
        }

        return new HierarchyResult(totalInserted, orgUnitsByLevel);
    }

    private void insertOrgUnitGroupMemberships(Long orgunitgroupid, List<Object> orgUnitIds) throws SQLException {
        log.info("Inserting {} org unit group memberships for group {}", orgUnitIds.size(), orgunitgroupid);

        String sql = "INSERT INTO orgunitgroupmembers (orgunitgroupid, organisationunitid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
                for (Object orgUnitId : orgUnitIds) {
                    ps.setLong(1, orgunitgroupid);
                    ps.setLong(2, ((Number) orgUnitId).longValue());
                    ps.addBatch();
                    batchCount++;

                    if (batchCount % 1000 == 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }

                if (batchCount % 1000 != 0) {
                    ps.executeBatch();
                }

                conn.commit();
                log.info("Successfully inserted {} org unit group memberships", orgUnitIds.size());

            } catch (SQLException e) {
                conn.rollback();
                log.error("Failed to insert org unit group memberships, rolling back", e);
                throw e;
            }
        }
    }

    private void insertAutoOrgUnitGroupsPerLevel(List<List<Object>> orgUnitsByLevel) throws SQLException {
        TableMetadata groupTable = schemaService.getTable("orgunitgroup");
        if (groupTable == null) {
            log.warn("orgunitgroup table not found — skipping auto org unit group creation");
            return;
        }

        List<ColumnMetadata> groupColumns = groupTable.getInsertableColumns();
        String groupInsertSql = buildInsertSql("orgunitgroup", groupColumns, null);
        String memberInsertSql = "INSERT INTO orgunitgroupmembers (orgunitgroupid, organisationunitid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long nextGroupId = getMaxPk(conn, "orgunitgroup", "orgunitgroupid") + 1;

                for (int level = 0; level < orgUnitsByLevel.size(); level++) {
                    long groupId = nextGroupId + level;
                    List<Object> levelOrgUnitIds = orgUnitsByLevel.get(level);

                    Map<String, Object> groupRow = dataGenerator.generateRow(groupTable);
                    groupRow.put("orgunitgroupid", groupId);
                    groupRow.put("name", "Level-" + (level + 1) + "-Group-" + generateRandomSuffix());

                    try (PreparedStatement groupPs = conn.prepareStatement(groupInsertSql)) {
                        setParameters(groupPs, groupColumns, groupRow);
                        groupPs.executeUpdate();
                    }

                    try (PreparedStatement memberPs = conn.prepareStatement(memberInsertSql)) {
                        int batchCount = 0;
                        for (Object orgUnitId : levelOrgUnitIds) {
                            memberPs.setLong(1, groupId);
                            memberPs.setLong(2, ((Number) orgUnitId).longValue());
                            memberPs.addBatch();
                            batchCount++;

                            if (batchCount % 1000 == 0) {
                                memberPs.executeBatch();
                                memberPs.clearBatch();
                            }
                        }
                        if (batchCount % 1000 != 0) {
                            memberPs.executeBatch();
                        }
                    }

                    conn.commit();
                    log.info("Created org unit group {} (level {}) with {} members",
                        groupId, level + 1, levelOrgUnitIds.size());
                }
            } catch (SQLException e) {
                conn.rollback();
                log.error("Auto org unit group creation failed, rolling back", e);
                throw e;
            }
        }
    }

    private long getMaxPk(Connection conn, String table, String pkColumn) throws SQLException {
        String sql = "SELECT COALESCE(MAX(" + pkColumn + "), 0) FROM " + table;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getLong(1);
            return 0;
        }
    }

    private void addColumnIfMissing(List<ColumnMetadata> columnsToInsert, TableMetadata table, String columnName) {
        boolean alreadyPresent = columnsToInsert.stream()
            .anyMatch(c -> c.name().equalsIgnoreCase(columnName));
        if (!alreadyPresent) {
            table.columns().stream()
                .filter(c -> c.name().equalsIgnoreCase(columnName))
                .findFirst()
                .ifPresent(columnsToInsert::add);
        }
    }

    private record InsertedRow(Object id, String path) {}

    private List<InsertedRow> insertLevel(Connection conn, String sql, TableMetadata table,
                                      List<ColumnMetadata> columns, ColumnMetadata pkColumn,
                                      String parentColumn, Object parentId, String parentPath,
                                      int hierarchyLevel, boolean isOrgUnit, int count) throws SQLException {
        List<InsertedRow> insertedRows = new ArrayList<>();
        List<String> batchPaths = new ArrayList<>();
        // Track self-generated PKs when column is not auto-increment
        boolean selfGeneratedPk = pkColumn != null && !pkColumn.isAutoIncrement();
        List<Object> batchPkValues = selfGeneratedPk ? new ArrayList<>() : null;

        try (PreparedStatement ps = selfGeneratedPk
                ? conn.prepareStatement(sql)
                : conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);

                // Override parent column value
                row.put(parentColumn, parentId);

                // For organisationunit, set path, hierarchylevel, and openingdate
                String currentPath = null;
                if (isOrgUnit) {
                    String uid = (String) row.get("uid");
                    if (parentPath == null) {
                        currentPath = "/" + uid;
                    } else {
                        currentPath = parentPath + "/" + uid;
                    }
                    row.put("path", currentPath);
                    row.put("hierarchylevel", hierarchyLevel);
                    row.put("openingdate", Timestamp.valueOf("2024-01-01 00:00:00"));
                    batchPaths.add(currentPath);
                }

                // Capture self-generated PK value before inserting
                if (selfGeneratedPk) {
                    Object pkValue = row.get(pkColumn.name());
                    batchPkValues.add(pkValue);
                }

                setParameters(ps, columns, row);
                ps.addBatch();

                if ((i + 1) % 1000 == 0 || i == count - 1) {
                    int batchStartIndex = insertedRows.size();
                    ps.executeBatch();

                    if (selfGeneratedPk) {
                        // Use self-generated PK values
                        for (int idx = 0; idx < batchPkValues.size(); idx++) {
                            Object id = batchPkValues.get(idx);
                            String path = isOrgUnit ? batchPaths.get(batchStartIndex + idx) : null;
                            insertedRows.add(new InsertedRow(id, path));
                        }
                        log.debug("Captured {} self-generated keys from batch", batchPkValues.size());
                        batchPkValues.clear();
                    } else {
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            int idx = 0;
                            while (keys.next()) {
                                Object id = keys.getObject(1);
                                String path = isOrgUnit ? batchPaths.get(batchStartIndex + idx) : null;
                                insertedRows.add(new InsertedRow(id, path));
                                idx++;
                            }
                            log.debug("Captured {} generated keys from batch", idx);
                        }
                    }

                    ps.clearBatch();
                }
            }
        }

        return insertedRows;
    }

    private String buildInsertSql(String tableName, List<ColumnMetadata> columns, String pkColumn) {
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

    private String generateRandomSuffix() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            sb.append(chars.charAt(java.util.concurrent.ThreadLocalRandom.current().nextInt(chars.length())));
        }
        return sb.toString();
    }

    public record HierarchyResult(int totalInserted, List<List<Object>> orgUnitsByLevel) {}

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
