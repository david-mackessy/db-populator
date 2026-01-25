package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.TableMetadata;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SchemaDetectionService {

    private static final Logger log = LoggerFactory.getLogger(SchemaDetectionService.class);

    private final DataSource dataSource;
    private final Map<String, TableMetadata> tableCache = new ConcurrentHashMap<>();

    public SchemaDetectionService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void init() {
        log.info("Starting schema detection...");
        try {
            detectSchema();
            log.info("Schema detection completed. Found {} tables", tableCache.size());
        } catch (SQLException e) {
            log.error("Failed to detect schema", e);
            throw new RuntimeException("Schema detection failed", e);
        }
    }

    public void detectSchema() throws SQLException {
        log.debug("Connecting to database for schema detection");
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String catalog = conn.getCatalog();
            String schema = "public";
            log.debug("Connected to catalog: {}, schema: {}", catalog, schema);

            Set<String> tableNames = getTableNames(metaData, catalog, schema);
            log.info("Found {} tables in schema", tableNames.size());
            Map<String, Set<String>> primaryKeys = new HashMap<>();
            Map<String, Set<String>> autoIncrementColumns = new HashMap<>();
            Map<String, Map<String, ForeignKeyInfo>> foreignKeys = new HashMap<>();

            Map<String, Set<String>> generatedColumns = new HashMap<>();

            for (String tableName : tableNames) {
                log.debug("Detecting metadata for table: {}", tableName);
                try {
                    primaryKeys.put(tableName, getPrimaryKeys(metaData, catalog, schema, tableName));
                    foreignKeys.put(tableName, getForeignKeys(metaData, catalog, schema, tableName));
                    autoIncrementColumns.put(tableName, getAutoIncrementColumns(conn, schema, tableName));
                    generatedColumns.put(tableName, getGeneratedColumns(conn, schema, tableName));
                } catch (SQLException e) {
                    log.error("Failed to detect metadata for table: {}", tableName, e);
                    throw e;
                }
            }

            for (String tableName : tableNames) {
                List<ColumnMetadata> columns = getColumns(
                    metaData, catalog, schema, tableName,
                    primaryKeys.get(tableName),
                    autoIncrementColumns.get(tableName),
                    generatedColumns.get(tableName),
                    foreignKeys.get(tableName)
                );

                // Only include dependencies for FK columns that are actually insertable
                // (non-nullable, non-PK, non-auto-increment, non-generated)
                Set<String> dependsOn = new HashSet<>();
                for (ColumnMetadata col : columns) {
                    if (col.isForeignKey()
                            && !col.referencedTable().equals(tableName)
                            && !col.nullable()
                            && !col.isPrimaryKey()
                            && !col.isAutoIncrement()
                            && !col.isGenerated()) {
                        dependsOn.add(col.referencedTable());
                    }
                }

                tableCache.put(tableName, new TableMetadata(tableName, schema, columns, dependsOn));
            }
        }
    }

    private Set<String> getTableNames(DatabaseMetaData metaData, String catalog, String schema) throws SQLException {
        Set<String> tables = new HashSet<>();
        try (ResultSet rs = metaData.getTables(catalog, schema, null, new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    private Set<String> getPrimaryKeys(DatabaseMetaData metaData, String catalog, String schema, String tableName)
            throws SQLException {
        Set<String> pks = new HashSet<>();
        try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName)) {
            while (rs.next()) {
                pks.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pks;
    }

    private Set<String> getAutoIncrementColumns(Connection conn, String schema, String tableName) throws SQLException {
        Set<String> autoIncrement = new HashSet<>();
        String sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            AND (column_default LIKE 'nextval%' OR is_identity = 'YES')
            """;
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    autoIncrement.add(rs.getString("column_name"));
                }
            }
        }
        return autoIncrement;
    }

    private Set<String> getGeneratedColumns(Connection conn, String schema, String tableName) throws SQLException {
        Set<String> generated = new HashSet<>();
        String sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            AND (is_generated = 'ALWAYS' OR generation_expression IS NOT NULL)
            """;
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    generated.add(rs.getString("column_name"));
                }
            }
        }
        if (!generated.isEmpty()) {
            log.debug("Found {} generated columns in table {}: {}", generated.size(), tableName, generated);
        }
        return generated;
    }

    private Map<String, ForeignKeyInfo> getForeignKeys(DatabaseMetaData metaData, String catalog, String schema,
                                                       String tableName) throws SQLException {
        Map<String, ForeignKeyInfo> fks = new HashMap<>();
        try (ResultSet rs = metaData.getImportedKeys(catalog, schema, tableName)) {
            while (rs.next()) {
                String columnName = rs.getString("FKCOLUMN_NAME");
                String refTable = rs.getString("PKTABLE_NAME");
                String refColumn = rs.getString("PKCOLUMN_NAME");
                fks.put(columnName, new ForeignKeyInfo(refTable, refColumn));
            }
        }
        return fks;
    }

    private List<ColumnMetadata> getColumns(DatabaseMetaData metaData, String catalog, String schema, String tableName,
                                            Set<String> primaryKeys, Set<String> autoIncrementCols,
                                            Set<String> generatedCols,
                                            Map<String, ForeignKeyInfo> foreignKeys) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(catalog, schema, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                Integer columnSize = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    columnSize = null;
                }

                boolean isPk = primaryKeys.contains(columnName);
                boolean isAutoIncrement = autoIncrementCols.contains(columnName);
                boolean isGenerated = generatedCols.contains(columnName);
                ForeignKeyInfo fkInfo = foreignKeys.get(columnName);

                columns.add(new ColumnMetadata(
                    columnName,
                    dataType,
                    sqlType,
                    nullable,
                    isPk,
                    isAutoIncrement,
                    isGenerated,
                    columnSize,
                    fkInfo != null ? fkInfo.referencedTable() : null,
                    fkInfo != null ? fkInfo.referencedColumn() : null
                ));
            }
        }
        return columns;
    }

    public TableMetadata getTable(String tableName) {
        return tableCache.get(tableName);
    }

    public Collection<TableMetadata> getAllTables() {
        return Collections.unmodifiableCollection(tableCache.values());
    }

    public boolean tableExists(String tableName) {
        return tableCache.containsKey(tableName);
    }

    public void refreshSchema() throws SQLException {
        log.info("Refreshing schema cache");
        tableCache.clear();
        try {
            detectSchema();
            log.info("Schema refresh completed successfully. {} tables cached", tableCache.size());
        } catch (SQLException e) {
            log.error("Failed to refresh schema", e);
            throw e;
        }
    }

    public long getTableCount(String tableName) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Failed to get count for table {}", tableName, e);
        }
        return 0;
    }

    public int makeAllPrimaryKeysAutoGenerated() throws SQLException {
        log.info("Making all primary keys auto-generated...");
        int updated = 0;
        int skipped = 0;

        for (TableMetadata table : tableCache.values()) {
            ColumnMetadata pk = table.getPrimaryKeyColumn();
            if (pk == null) {
                log.debug("Table {} has no primary key, skipping", table.tableName());
                continue;
            }

            if (table.hasCompositePrimaryKey()) {
                log.debug("Table {} has composite primary key, skipping", table.tableName());
                skipped++;
                continue;
            }

            if (pk.isAutoIncrement()) {
                // Even if already auto-increment, reset sequence to max value
                resetIdentitySequence(table.tableName(), pk.name());
                skipped++;
                continue;
            }

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    // Get max value first
                    long maxId = getMaxColumnValue(conn, table.tableName(), pk.name());

                    // Add identity to the column
                    String alterSql = String.format(
                        "ALTER TABLE %s ALTER COLUMN %s ADD GENERATED BY DEFAULT AS IDENTITY",
                        table.tableName(), pk.name());

                    try (var stmt = conn.createStatement()) {
                        stmt.execute(alterSql);
                    }

                    // Restart the identity to max + 1
                    String restartSql = String.format(
                        "ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d",
                        table.tableName(), pk.name(), maxId + 1);

                    try (var stmt = conn.createStatement()) {
                        stmt.execute(restartSql);
                    }

                    conn.commit();
                    log.info("Set auto-generation for {}.{} starting at {}", table.tableName(), pk.name(), maxId + 1);
                    updated++;
                } catch (SQLException e) {
                    conn.rollback();
                    log.warn("Could not set identity for {}.{}: {}",
                        table.tableName(), pk.name(), e.getMessage());
                    skipped++;
                }
            }
        }

        log.info("Successfully made {} primary keys auto-generated, {} skipped", updated, skipped);

        // Refresh schema to pick up changes
        refreshSchema();

        return updated;
    }

    private long getMaxColumnValue(Connection conn, String tableName, String columnName) throws SQLException {
        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", columnName, tableName);
        try (var stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    private void resetIdentitySequence(String tableName, String columnName) {
        try (Connection conn = dataSource.getConnection()) {
            long maxId = getMaxColumnValue(conn, tableName, columnName);

            String restartSql = String.format(
                "ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d",
                tableName, columnName, maxId + 1);

            try (var stmt = conn.createStatement()) {
                stmt.execute(restartSql);
            }
            log.debug("Reset identity for {}.{} to {}", tableName, columnName, maxId + 1);
        } catch (SQLException e) {
            log.warn("Could not reset identity for {}.{}: {}", tableName, columnName, e.getMessage());
        }
    }

    private record ForeignKeyInfo(String referencedTable, String referencedColumn) {}
}
