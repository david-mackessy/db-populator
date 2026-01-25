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

@Service
public class CategoryModelInsertService {

    private static final Logger log = LoggerFactory.getLogger(CategoryModelInsertService.class);

    private final DataSource dataSource;
    private final DataGeneratorService dataGenerator;
    private final SchemaDetectionService schemaService;

    public CategoryModelInsertService(DataSource dataSource,
                                       DataGeneratorService dataGenerator,
                                       SchemaDetectionService schemaService) {
        this.dataSource = dataSource;
        this.dataGenerator = dataGenerator;
        this.schemaService = schemaService;
    }

    public int insertCategoryModel(int combos, int categoriesPerCombo,
                                    int optionsPerCategory, ProgressCallback callback) throws SQLException {
        log.info("Starting category model insert: {} combos, {} categories/combo, {} options/category",
            combos, categoriesPerCombo, optionsPerCategory);

        // Detect join tables
        String categoryOptionJoinTable = findJoinTable("category", "categoryoption");
        String comboCategoryJoinTable = findJoinTable("categorycombo", "category");

        if (categoryOptionJoinTable == null) {
            throw new IllegalArgumentException("Cannot find join table between category and categoryoption");
        }
        if (comboCategoryJoinTable == null) {
            throw new IllegalArgumentException("Cannot find join table between categorycombo and category");
        }

        log.info("Detected join tables: category-option={}, combo-category={}",
            categoryOptionJoinTable, comboCategoryJoinTable);

        int totalInserted = 0;
        int totalCategories = combos * categoriesPerCombo;
        int totalOptions = totalCategories * optionsPerCategory;

        // Store IDs for creating join table entries
        List<Long> categoryComboIds = new ArrayList<>();
        List<Long> categoryIds = new ArrayList<>();
        List<Long> categoryOptionIds = new ArrayList<>();

        // Track which categories belong to which combo, and which options belong to which category
        Map<Long, List<Long>> comboToCategories = new LinkedHashMap<>();
        Map<Long, List<Long>> categoryToOptions = new LinkedHashMap<>();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Step 1: Insert all CategoryOptions
                log.info("Inserting {} category options", totalOptions);
                categoryOptionIds = insertEntities(conn, "categoryoption", totalOptions);
                totalInserted += categoryOptionIds.size();
                if (callback != null) {
                    callback.onProgress(totalInserted);
                }
                log.info("Inserted {} category options", categoryOptionIds.size());

                // Step 2: Insert all Categories and track which options belong to each
                log.info("Inserting {} categories", totalCategories);
                int optionIndex = 0;
                for (int c = 0; c < totalCategories; c++) {
                    List<Long> ids = insertEntities(conn, "category", 1);
                    Long categoryId = ids.get(0);
                    categoryIds.add(categoryId);

                    // Assign options to this category
                    List<Long> optionsForCategory = new ArrayList<>();
                    for (int o = 0; o < optionsPerCategory; o++) {
                        optionsForCategory.add(categoryOptionIds.get(optionIndex++));
                    }
                    categoryToOptions.put(categoryId, optionsForCategory);

                    totalInserted++;
                    if (callback != null && (c + 1) % 10 == 0) {
                        callback.onProgress(totalInserted);
                    }
                }
                if (callback != null) {
                    callback.onProgress(totalInserted);
                }
                log.info("Inserted {} categories", categoryIds.size());

                // Step 3: Insert all CategoryCombos and track which categories belong to each
                log.info("Inserting {} category combos", combos);
                int categoryIndex = 0;
                for (int cc = 0; cc < combos; cc++) {
                    List<Long> ids = insertEntities(conn, "categorycombo", 1);
                    Long comboId = ids.get(0);
                    categoryComboIds.add(comboId);

                    // Assign categories to this combo
                    List<Long> categoriesForCombo = new ArrayList<>();
                    for (int cat = 0; cat < categoriesPerCombo; cat++) {
                        categoriesForCombo.add(categoryIds.get(categoryIndex++));
                    }
                    comboToCategories.put(comboId, categoriesForCombo);

                    totalInserted++;
                    if (callback != null) {
                        callback.onProgress(totalInserted);
                    }
                }
                log.info("Inserted {} category combos", categoryComboIds.size());

                conn.commit();

                // Step 4: Insert category -> categoryoption join table entries
                log.info("Inserting {} category-option join entries", totalOptions);
                int joinCount = insertCategoryOptionJoinEntries(categoryOptionJoinTable, categoryToOptions);
                log.info("Inserted {} category-option join entries", joinCount);

                // Step 5: Insert categorycombo -> category join table entries
                log.info("Inserting {} combo-category join entries", totalCategories);
                int comboJoinCount = insertComboCategoryJoinEntries(comboCategoryJoinTable, comboToCategories);
                log.info("Inserted {} combo-category join entries", comboJoinCount);

            } catch (SQLException e) {
                conn.rollback();
                log.error("Category model insert failed, rolling back", e);
                throw e;
            }
        }

        log.info("Category model insert complete: {} total entity rows", totalInserted);
        return totalInserted;
    }

    private String findJoinTable(String table1, String table2) {
        for (TableMetadata table : schemaService.getAllTables()) {
            if (table.hasCompositePrimaryKey()) {
                List<ColumnMetadata> fkCols = table.getForeignKeyColumns();
                Set<String> referencedTables = new HashSet<>();
                for (ColumnMetadata col : fkCols) {
                    if (col.referencedTable() != null) {
                        referencedTables.add(col.referencedTable().toLowerCase());
                    }
                }
                if (referencedTables.contains(table1.toLowerCase()) && referencedTables.contains(table2.toLowerCase())) {
                    log.debug("Found join table {} for {} and {}", table.tableName(), table1, table2);
                    return table.tableName();
                }
            }
        }

        // Fallback: check for tables with naming convention containing both table names
        for (TableMetadata table : schemaService.getAllTables()) {
            String tableName = table.tableName().toLowerCase();
            if ((tableName.contains(table1.toLowerCase()) && tableName.contains(table2.toLowerCase())) ||
                tableName.equals(table1.toLowerCase() + table2.toLowerCase() + "s") ||
                tableName.equals(table2.toLowerCase() + table1.toLowerCase() + "s")) {
                log.debug("Found join table {} by naming convention for {} and {}", table.tableName(), table1, table2);
                return table.tableName();
            }
        }

        return null;
    }

    private List<Long> insertEntities(Connection conn, String tableName, int count) throws SQLException {
        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        ColumnMetadata pk = table.getPrimaryKeyColumn();
        boolean selfGeneratedPk = pk != null && !pk.isAutoIncrement();

        List<ColumnMetadata> insertableColumns = table.getInsertableColumns();
        String sql = buildInsertSql(tableName, insertableColumns);
        List<Long> insertedIds = new ArrayList<>();
        List<Long> batchPkValues = selfGeneratedPk ? new ArrayList<>() : null;

        try (PreparedStatement ps = selfGeneratedPk
                ? conn.prepareStatement(sql)
                : conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < count; i++) {
                Map<String, Object> row = dataGenerator.generateRow(table);

                // Capture self-generated PK value before inserting
                if (selfGeneratedPk && pk != null) {
                    Object pkValue = row.get(pk.name());
                    if (pkValue instanceof Number num) {
                        batchPkValues.add(num.longValue());
                    }
                }

                setParameters(ps, insertableColumns, row);
                ps.addBatch();

                if ((i + 1) % 1000 == 0 || i == count - 1) {
                    ps.executeBatch();

                    if (selfGeneratedPk) {
                        // Use self-generated PK values
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

    private int insertCategoryOptionJoinEntries(String joinTableName, Map<Long, List<Long>> categoryToOptions) throws SQLException {
        TableMetadata joinTable = schemaService.getTable(joinTableName);
        if (joinTable == null) {
            throw new IllegalArgumentException("Join table not found: " + joinTableName);
        }

        // Find the FK columns for category and categoryoption
        String categoryFkCol = null;
        String optionFkCol = null;
        String sortOrderCol = null;

        for (ColumnMetadata col : joinTable.columns()) {
            if (col.isForeignKey()) {
                if ("category".equalsIgnoreCase(col.referencedTable())) {
                    categoryFkCol = col.name();
                } else if ("categoryoption".equalsIgnoreCase(col.referencedTable())) {
                    optionFkCol = col.name();
                }
            } else if (col.name().toLowerCase().contains("sort") || col.name().toLowerCase().contains("order")) {
                sortOrderCol = col.name();
            }
        }

        if (categoryFkCol == null || optionFkCol == null) {
            throw new IllegalArgumentException("Cannot determine FK columns for join table: " + joinTableName);
        }

        String sql;
        boolean hasSortOrder = sortOrderCol != null;
        if (hasSortOrder) {
            sql = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
                joinTableName, categoryFkCol, optionFkCol, sortOrderCol);
        } else {
            sql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)",
                joinTableName, categoryFkCol, optionFkCol);
        }

        int inserted = 0;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
                for (Map.Entry<Long, List<Long>> entry : categoryToOptions.entrySet()) {
                    Long categoryId = entry.getKey();
                    int sortOrder = 1;
                    for (Long optionId : entry.getValue()) {
                        ps.setLong(1, categoryId);
                        ps.setLong(2, optionId);
                        if (hasSortOrder) {
                            ps.setInt(3, sortOrder++);
                        }
                        ps.addBatch();
                        batchCount++;
                        inserted++;

                        if (batchCount % 1000 == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                }

                if (batchCount % 1000 != 0) {
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

    private int insertComboCategoryJoinEntries(String joinTableName, Map<Long, List<Long>> comboToCategories) throws SQLException {
        TableMetadata joinTable = schemaService.getTable(joinTableName);
        if (joinTable == null) {
            throw new IllegalArgumentException("Join table not found: " + joinTableName);
        }

        // Find the FK columns for categorycombo and category
        String comboFkCol = null;
        String categoryFkCol = null;
        String sortOrderCol = null;

        for (ColumnMetadata col : joinTable.columns()) {
            if (col.isForeignKey()) {
                if ("categorycombo".equalsIgnoreCase(col.referencedTable())) {
                    comboFkCol = col.name();
                } else if ("category".equalsIgnoreCase(col.referencedTable())) {
                    categoryFkCol = col.name();
                }
            } else if (col.name().toLowerCase().contains("sort") || col.name().toLowerCase().contains("order")) {
                sortOrderCol = col.name();
            }
        }

        if (comboFkCol == null || categoryFkCol == null) {
            throw new IllegalArgumentException("Cannot determine FK columns for join table: " + joinTableName);
        }

        String sql;
        boolean hasSortOrder = sortOrderCol != null;
        if (hasSortOrder) {
            sql = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
                joinTableName, comboFkCol, categoryFkCol, sortOrderCol);
        } else {
            sql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)",
                joinTableName, comboFkCol, categoryFkCol);
        }

        int inserted = 0;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
                for (Map.Entry<Long, List<Long>> entry : comboToCategories.entrySet()) {
                    Long comboId = entry.getKey();
                    int sortOrder = 1;
                    for (Long categoryId : entry.getValue()) {
                        ps.setLong(1, comboId);
                        ps.setLong(2, categoryId);
                        if (hasSortOrder) {
                            ps.setInt(3, sortOrder++);
                        }
                        ps.addBatch();
                        batchCount++;
                        inserted++;

                        if (batchCount % 1000 == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                }

                if (batchCount % 1000 != 0) {
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
