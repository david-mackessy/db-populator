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

    public int insertHierarchy(String tableName, String parentColumn, List<Integer> hierarchy,
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

        // For organisationunit table, ensure path and hierarchylevel columns are included
        boolean isOrgUnit = tableName.equalsIgnoreCase("organisationunit");
        if (isOrgUnit) {
            addColumnIfMissing(columnsToInsert, table, "path");
            addColumnIfMissing(columnsToInsert, table, "hierarchylevel");
        }

        String sql = buildInsertSql(tableName, columnsToInsert, pkColumn.name());
        log.debug("Hierarchy insert SQL: {}", sql);

        int totalInserted = 0;
        List<Object> currentLevelIds = new ArrayList<>();
        List<Object> allInsertedIds = new ArrayList<>();
        // Track id -> path mapping for organisationunit
        Map<Object, String> idToPath = new HashMap<>();

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

        // If orgunitgroupid is provided, create group memberships
        log.debug("Group membership check - isOrgUnit: {}, orgunitgroupid: {}, allInsertedIds size: {}",
            isOrgUnit, orgunitgroupid, allInsertedIds.size());
        if (isOrgUnit && orgunitgroupid != null && !allInsertedIds.isEmpty()) {
            insertOrgUnitGroupMemberships(orgunitgroupid, allInsertedIds);
        } else if (orgunitgroupid != null) {
            log.warn("Skipping group membership insert - isOrgUnit: {}, allInsertedIds empty: {}",
                isOrgUnit, allInsertedIds.isEmpty());
        }

        return totalInserted;
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

        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);

                // Override parent column value
                row.put(parentColumn, parentId);

                // For organisationunit, set path and hierarchylevel
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
                    batchPaths.add(currentPath);
                }

                setParameters(ps, columns, row);
                ps.addBatch();

                if ((i + 1) % 1000 == 0 || i == count - 1) {
                    int batchStartIndex = insertedRows.size();
                    ps.executeBatch();

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

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
