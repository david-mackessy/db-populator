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
    private final CategoryDimensionInsertService categoryDimensionInsertService;
    private final DataElementInsertService dataElementInsertService;
    private final DataSetInsertService dataSetInsertService;
    private final DataSetElementInsertService dataSetElementInsertService;
    private final ProgramInsertService programInsertService;
    private final JobTracker jobTracker;

    public AsyncJobExecutor(DependencyResolver dependencyResolver,
                            BatchInsertService batchInsertService,
                            DataGeneratorService dataGeneratorService,
                            HierarchyInsertService hierarchyInsertService,
                            CategoryModelInsertService categoryModelInsertService,
                            CategoryDimensionInsertService categoryDimensionInsertService,
                            DataElementInsertService dataElementInsertService,
                            DataSetInsertService dataSetInsertService,
                            DataSetElementInsertService dataSetElementInsertService,
                            ProgramInsertService programInsertService,
                            JobTracker jobTracker) {
        this.dependencyResolver = dependencyResolver;
        this.batchInsertService = batchInsertService;
        this.dataGeneratorService = dataGeneratorService;
        this.hierarchyInsertService = hierarchyInsertService;
        this.categoryModelInsertService = categoryModelInsertService;
        this.categoryDimensionInsertService = categoryDimensionInsertService;
        this.dataElementInsertService = dataElementInsertService;
        this.dataSetInsertService = dataSetInsertService;
        this.dataSetElementInsertService = dataSetElementInsertService;
        this.programInsertService = programInsertService;
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

    @Async("populatorExecutor")
    public void executeDataElementJobAsync(String jobId, int amount, List<Long> categoryComboIds,
                                            String valueType, String domainType, String aggregationType) {
        log.info("Starting async dataelement job {} with {} rows and {} categorycomboid values",
            jobId, amount, categoryComboIds.size());

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "dataelement", amount, false);

        try {
            int inserted = dataElementInsertService.insertDataElements(amount, categoryComboIds,
                valueType, domainType, aggregationType,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "dataelement", totalInserted));

            jobTracker.updateTableProgress(jobId, "dataelement", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} rows inserted in dataelement",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    @Async("populatorExecutor")
    public void executeDataSetJobAsync(String jobId, int amount, List<Long> categoryComboIds, List<Long> periodTypeIds) {
        log.info("Starting async dataset job {} with {} rows, {} categorycomboid values, {} periodTypeIds",
            jobId, amount, categoryComboIds.size(), periodTypeIds.size());

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "dataset", amount, false);

        try {
            int inserted = dataSetInsertService.insertDataSets(amount, categoryComboIds, periodTypeIds,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "dataset", totalInserted));

            jobTracker.updateTableProgress(jobId, "dataset", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} rows inserted in dataset",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    @Async("populatorExecutor")
    public void executeProgramJobAsync(String jobId, int amount, List<Long> categoryComboIds, String programType) {
        log.info("Starting async program job {} with {} rows, {} categorycomboid values, programType={}",
            jobId, amount, categoryComboIds.size(), programType);

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "program", amount, false);

        try {
            int inserted = programInsertService.insertPrograms(amount, categoryComboIds, programType,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "program", totalInserted));

            jobTracker.updateTableProgress(jobId, "program", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} rows inserted in program",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    @Async("populatorExecutor")
    public void executeDataSetElementJobAsync(String jobId, int amount, List<Long> categoryComboIds) {
        int totalExpected = amount * 3;
        log.info("Starting async datasetelement job {} with {} dataelements + {} datasets + {} join rows",
            jobId, amount, amount, amount);

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "datasetelement", totalExpected, false);

        try {
            int inserted = dataSetElementInsertService.insertDataSetElements(amount, categoryComboIds,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "datasetelement", totalInserted));

            jobTracker.updateTableProgress(jobId, "datasetelement", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} total rows inserted for datasetelement",
                jobId, inserted);

        } catch (IllegalArgumentException e) {
            log.error("Job {} failed - invalid argument: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        } catch (Exception e) {
            log.error("Job {} failed with unexpected error: {}", jobId, e.getMessage(), e);
            jobTracker.markFailed(jobId, e.getMessage());
        }
    }

    @Async("populatorExecutor")
    public void executeCategoryDimensionJobAsync(String jobId, int amount, List<Long> categoryIds) {
        log.info("Starting async category dimension job {} with {} rows and {} category IDs",
            jobId, amount, categoryIds.size());

        jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
        jobTracker.registerTable(jobId, "categorydimension", amount, false);

        try {
            int inserted = categoryDimensionInsertService.insertCategoryDimensions(amount, categoryIds,
                (totalInserted) -> jobTracker.updateTableProgress(jobId, "categorydimension", totalInserted));

            jobTracker.updateTableProgress(jobId, "categorydimension", inserted);
            jobTracker.markCompleted(jobId);
            log.info("Job {} completed successfully: {} rows inserted in categorydimension",
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
