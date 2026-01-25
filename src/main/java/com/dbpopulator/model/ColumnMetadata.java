package com.dbpopulator.model;

public record ColumnMetadata(
    String name,
    String dataType,
    int sqlType,
    boolean nullable,
    boolean isPrimaryKey,
    boolean isAutoIncrement,
    boolean isGenerated,
    Integer columnSize,
    String referencedTable,
    String referencedColumn
) {
    public boolean isForeignKey() {
        return referencedTable != null && referencedColumn != null;
    }
}
