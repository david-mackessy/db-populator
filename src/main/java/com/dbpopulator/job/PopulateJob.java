package com.dbpopulator.job;

import com.dbpopulator.model.JobStatus;
import com.dbpopulator.model.TableInsertCount;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PopulateJob {

    private final String jobId;
    private final String targetTable;
    private final int requestedRows;
    private volatile JobStatus.Status status;
    private volatile String currentTable;
    private volatile String error;
    private final Instant startedAt;
    private volatile Instant completedAt;
    private final Map<String, TableInsertInfo> tableInserts = new ConcurrentHashMap<>();

    public PopulateJob(String targetTable, int requestedRows) {
        this.jobId = UUID.randomUUID().toString();
        this.targetTable = targetTable;
        this.requestedRows = requestedRows;
        this.status = JobStatus.Status.PENDING;
        this.startedAt = Instant.now();
    }

    private static class TableInsertInfo {
        final int requested;
        final boolean isDependency;
        volatile int inserted;

        TableInsertInfo(int requested, boolean isDependency) {
            this.requested = requested;
            this.isDependency = isDependency;
            this.inserted = 0;
        }
    }

    public String getJobId() {
        return jobId;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public int getRequestedRows() {
        return requestedRows;
    }

    public JobStatus.Status getStatus() {
        return status;
    }

    public void setStatus(JobStatus.Status status) {
        this.status = status;
        if (status == JobStatus.Status.COMPLETED || status == JobStatus.Status.FAILED) {
            this.completedAt = Instant.now();
        }
    }

    public String getCurrentTable() {
        return currentTable;
    }

    public void setCurrentTable(String currentTable) {
        this.currentTable = currentTable;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void registerTable(String tableName, int requested, boolean isDependency) {
        tableInserts.put(tableName, new TableInsertInfo(requested, isDependency));
    }

    public void updateTableInserted(String tableName, int inserted) {
        TableInsertInfo info = tableInserts.get(tableName);
        if (info != null) {
            info.inserted = inserted;
        }
    }

    public int getTargetTableInserted() {
        TableInsertInfo info = tableInserts.get(targetTable);
        return info != null ? info.inserted : 0;
    }

    public int getTotalInserted() {
        return tableInserts.values().stream().mapToInt(i -> i.inserted).sum();
    }

    public int getProgress() {
        if (requestedRows == 0) {
            return 0;
        }
        int targetInserted = getTargetTableInserted();
        return (int) ((targetInserted * 100L) / requestedRows);
    }

    public List<TableInsertCount> getTableInsertCounts() {
        List<TableInsertCount> counts = new ArrayList<>();
        for (Map.Entry<String, TableInsertInfo> entry : tableInserts.entrySet()) {
            TableInsertInfo info = entry.getValue();
            counts.add(new TableInsertCount(
                entry.getKey(),
                info.requested,
                info.inserted,
                info.isDependency
            ));
        }
        counts.sort((a, b) -> {
            if (a.isDependency() != b.isDependency()) {
                return a.isDependency() ? 1 : -1;
            }
            return a.tableName().compareTo(b.tableName());
        });
        return counts;
    }

    public JobStatus toJobStatus() {
        return new JobStatus(
            jobId,
            status,
            getProgress(),
            targetTable,
            requestedRows,
            getTargetTableInserted(),
            getTableInsertCounts(),
            currentTable,
            error,
            startedAt,
            completedAt
        );
    }
}
