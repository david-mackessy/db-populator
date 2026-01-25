package com.dbpopulator.service;

import com.dbpopulator.job.JobTracker;
import com.dbpopulator.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AsyncJobExecutor {

    private static final Logger log = LoggerFactory.getLogger(AsyncJobExecutor.class);

    private final DependencyResolver dependencyResolver;
    private final BatchInsertService batchInsertService;
    private final DataGeneratorService dataGeneratorService;
    private final HierarchyInsertService hierarchyInsertService;
    private final CategoryModelInsertService categoryModelInsertService;
    private final JobTracker jobTracker;

    public AsyncJobExecutor(DependencyResolver dependencyResolver,
                            BatchInsertService batchInsertService,
                            DataGeneratorService dataGeneratorService,
                            HierarchyInsertService hierarchyInsertService,
                            CategoryModelInsertService categoryModelInsertService,
                            JobTracker jobTracker) {
        this.dependencyResolver = dependencyResolver;
        this.batchInsertService = batchInsertService;
        this.dataGeneratorService = dataGeneratorService;
        this.hierarchyInsertService = hierarchyInsertService;
        this.categoryModelInsertService = categoryModelInsertService;
        this.jobTracker = jobTracker;
    }

    @Async("populatorExecutor")
    public void executeJobAsync(String jobId, String tableName, int amount) {
        log.info("Starting async execution of job {} for table {} with {} rows", jobId, tableName, amount);

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        log.debug("Job {} status changed to RUNNING", jobId);
        dataGeneratorService.clearForeignKeyCache();

        try {
            List<String> insertionOrder = dependencyResolver.resolveInsertionOrder(tableName);
            log.info("Job {} - insertion order for {}: {}", jobId, tableName, insertionOrder);

            // Register all tables with their planned insert counts
            for (String table : insertionOrder) {
                boolean isDependency = !table.equals(tableName);
                int rowsToInsert = isDependency ? calculateDependencyRows(amount) : amount;
                jobTracker.registerTable(jobId, table, rowsToInsert, isDependency);
            }

            for (int i = 0; i < insertionOrder.size(); i++) {
                String currentTable = insertionOrder.get(i);
                jobTracker.setCurrentTable(jobId, currentTable);
                log.debug("Job {} - processing table {}/{}: {}", jobId, i + 1, insertionOrder.size(), currentTable);

                boolean isTargetTable = currentTable.equals(tableName);
                int rowsToInsert = isTargetTable ? amount : calculateDependencyRows(amount);

                if (rowsToInsert > 0) {
                    log.info("Job {} - inserting {} rows into {} (dependency: {})",
                        jobId, rowsToInsert, currentTable, !isTargetTable);

                    try {
                        int inserted = batchInsertService.insertBatch(currentTable, rowsToInsert,
                            (current, total) -> jobTracker.updateTableProgress(jobId, currentTable, current));

                        jobTracker.updateTableProgress(jobId, currentTable, inserted);
                        log.debug("Job {} - completed {} rows for table {}",
                            jobId, inserted, currentTable);
                    } catch (Exception e) {
                        log.error("Job {} - failed to insert into table {}: {}", jobId, currentTable, e.getMessage(), e);
                        throw e;
                    }
                }
            }

            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully for target table {} with {} tables processed",
                jobId, tableName, insertionOrder.size());

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    private int calculateDependencyRows(int targetAmount) {
        return Math.max(1, targetAmount / 10);
    }

    @Async("populatorExecutor")
    public void executeHierarchyJobAsync(String jobId, String tableName, String parentColumn,
                                          List<Integer> hierarchy, Long orgunitgroupid) {
        int totalExpected = calculateHierarchyTotal(hierarchy);
        log.info("Starting async hierarchy job {} for table {} with {} levels, ~{} total rows",
            jobId, tableName, hierarchy.size(), totalExpected);

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, tableName, totalExpected, false);
        dataGeneratorService.clearForeignKeyCache();

        try {
            int inserted = hierarchyInsertService.insertHierarchy(tableName, parentColumn, hierarchy,
                orgunitgroupid,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, tableName, totalInserted));

            jobTracker.updateTableProgress(jobId, tableName, inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} rows inserted in hierarchy",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    private int calculateHierarchyTotal(List<Integer> hierarchy) {
        int total = 0;
        int multiplier = 1;
        for (int count : hierarchy) {
            multiplier *= count;
            total += multiplier;
        }
        return total;
    }

    @Async("populatorExecutor")
    public void executeCategoryModelJobAsync(String jobId, int combos,
                                              int categoriesPerCombo, int optionsPerCategory) {
        int totalExpected = combos + (combos * categoriesPerCombo) +
            (combos * categoriesPerCombo * optionsPerCategory);
        log.info("Starting async category model job {} with {} combos, {} cats/combo, {} opts/cat (~{} total entities)",
            jobId, combos, categoriesPerCombo, optionsPerCategory, totalExpected);

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "categorymodel", totalExpected, false);
        dataGeneratorService.clearForeignKeyCache();

        try {
            int inserted = categoryModelInsertService.insertCategoryModel(combos, categoriesPerCombo, optionsPerCategory,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "categorymodel", totalInserted));

            jobTracker.updateTableProgress(jobId, "categorymodel", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} entity rows inserted in category model",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }
}
