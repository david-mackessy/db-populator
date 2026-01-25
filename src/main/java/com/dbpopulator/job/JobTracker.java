package com.dbpopulator.job;

import com.dbpopulator.model.JobStatus;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class JobTracker {

    private final ConcurrentHashMap<String, PopulateJob> jobs = new ConcurrentHashMap<>();

    public PopulateJob createJob(String targetTable, int totalRows) {
        PopulateJob job = new PopulateJob(targetTable, totalRows);
        jobs.put(job.getJobId(), job);
        return job;
    }

    public Optional<PopulateJob> getJob(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public Optional<JobStatus> getJobStatus(String jobId) {
        return getJob(jobId).map(PopulateJob::toJobStatus);
    }

    public void updateJobStatus(String jobId, JobStatus.Status status) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.setStatus(status);
        }
    }

    public void registerTable(String jobId, String tableName, int requested, boolean isDependency) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.registerTable(tableName, requested, isDependency);
        }
    }

    public void updateTableProgress(String jobId, String tableName, int inserted) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.updateTableInserted(tableName, inserted);
            job.setCurrentTable(tableName);
        }
    }

    public void setCurrentTable(String jobId, String tableName) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.setCurrentTable(tableName);
        }
    }

    public void markFailed(String jobId, String error) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.setStatus(JobStatus.Status.FAILED);
            job.setError(error);
        }
    }

    public void markCompleted(String jobId) {
        PopulateJob job = jobs.get(jobId);
        if (job != null) {
            job.setStatus(JobStatus.Status.COMPLETED);
        }
    }

    public Collection<JobStatus> getAllJobs() {
        return jobs.values().stream()
            .map(PopulateJob::toJobStatus)
            .collect(Collectors.toList());
    }

    public Collection<JobStatus> getRunningJobs() {
        return jobs.values().stream()
            .filter(job -> job.getStatus() == JobStatus.Status.RUNNING)
            .map(PopulateJob::toJobStatus)
            .collect(Collectors.toList());
    }

    public void removeJob(String jobId) {
        jobs.remove(jobId);
    }

    public void clearCompletedJobs() {
        jobs.entrySet().removeIf(entry ->
            entry.getValue().getStatus() == JobStatus.Status.COMPLETED ||
            entry.getValue().getStatus() == JobStatus.Status.FAILED
        );
    }
}
