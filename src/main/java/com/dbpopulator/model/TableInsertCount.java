package com.dbpopulator.model;

public record TableInsertCount(
    String tableName,
    int requested,
    int inserted,
    boolean isDependency
) {
}
