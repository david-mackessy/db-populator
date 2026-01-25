package com.dbpopulator.model;

import java.time.Instant;
import java.util.List;

public record JobStatus(
    String jobId,
    Status status,
    int progress,
    String targetTable,
    int requestedRows,
    int targetTableInserted,
    List<TableInsertCount> tableInserts,
    String currentTable,
    String error,
    Instant startedAt,
    Instant completedAt
) {
    public enum Status {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED
    }

    public int getTotalInserted() {
        if (tableInserts == null) {
            return 0;
        }
        return tableInserts.stream().mapToInt(TableInsertCount::inserted).sum();
    }

    public int getDependencyInserted() {
        if (tableInserts == null) {
            return 0;
        }
        return tableInserts.stream()
            .filter(TableInsertCount::isDependency)
            .mapToInt(TableInsertCount::inserted)
            .sum();
    }
}
