package com.dbpopulator.model;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public record TableMetadata(
    String tableName,
    String schema,
    List<ColumnMetadata> columns,
    Set<String> dependsOn
) {
    public ColumnMetadata getPrimaryKeyColumn() {
        return columns.stream()
            .filter(ColumnMetadata::isPrimaryKey)
            .findFirst()
            .orElse(null);
    }

    public List<ColumnMetadata> getPrimaryKeyColumns() {
        return columns.stream()
            .filter(ColumnMetadata::isPrimaryKey)
            .collect(Collectors.toList());
    }

    public boolean hasCompositePrimaryKey() {
        return getPrimaryKeyColumns().size() > 1;
    }

    public List<ColumnMetadata> getForeignKeyColumns() {
        return columns.stream()
            .filter(ColumnMetadata::isForeignKey)
            .collect(Collectors.toList());
    }

    public List<ColumnMetadata> getInsertableColumns() {
        return columns.stream()
            .filter(col -> !col.isPrimaryKey())
            .filter(col -> !col.isAutoIncrement())
            .filter(col -> !col.isGenerated())
            .filter(col -> !col.nullable()
                || col.name().equalsIgnoreCase("uid")
                || col.name().equalsIgnoreCase("created")
                || isBooleanColumn(col))
            .collect(Collectors.toList());
    }

    private boolean isBooleanColumn(ColumnMetadata col) {
        return col.sqlType() == java.sql.Types.BOOLEAN
            || col.sqlType() == java.sql.Types.BIT
            || col.dataType().toLowerCase().contains("bool");
    }
}
