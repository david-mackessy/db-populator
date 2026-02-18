package com.dbpopulator.service;

import com.dbpopulator.job.JobTracker;
import com.dbpopulator.job.PopulateJob;
import com.dbpopulator.model.PopulateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PopulatorService {

    private static final Logger log = LoggerFactory.getLogger(PopulatorService.class);

    private final SchemaDetectionService schemaService;
    private final JobTracker jobTracker;
    private final AsyncJobExecutor asyncJobExecutor;

    public PopulatorService(SchemaDetectionService schemaService,
                            JobTracker jobTracker,
                            AsyncJobExecutor asyncJobExecutor) {
        this.schemaService = schemaService;
        this.jobTracker = jobTracker;
        this.asyncJobExecutor = asyncJobExecutor;
    }

    public PopulateJob startPopulateJob(PopulateRequest request) {
        if (request.isCategoryModel()) {
            return startCategoryModelJob(request);
        }

        if (request.isCategoryDimension()) {
            return startCategoryDimensionJob(request);
        }

        if (request.isDataSetElement()) {
            return startDataSetElementJob(request);
        }

        if (request.isDataElement()) {
            return startDataElementJob(request);
        }

        if (request.isDataSet()) {
            return startDataSetJob(request);
        }

        String tableName = request.type();

        if (!schemaService.tableExists(tableName)) {
            log.error("Cannot start populate job - table not found: {}", tableName);
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        if (request.hasHierarchy()) {
            return startHierarchyJob(tableName, request.hierarchy(), request.orgunitgroupid());
        } else {
            return startFlatJob(tableName, request.amount());
        }
    }

    private static final int MAX_OPTION_COMBINATIONS = 500;

    private PopulateJob startCategoryDimensionJob(PopulateRequest request) {
        if (!request.hasCategories()) {
            throw new IllegalArgumentException("Category dimension requires a non-empty 'categories' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Category dimension requires 'amount' > 0");
        }

        log.info("Received category dimension populate request: {} rows with {} category IDs",
            request.amount(), request.categories().size());

        PopulateJob job = jobTracker.createJob("categorydimension", request.amount());
        log.info("Created category dimension populate job {}", job.getJobId());

        asyncJobExecutor.executeCategoryDimensionJobAsync(job.getJobId(), request.amount(), request.categories());

        return job;
    }

    private PopulateJob startCategoryModelJob(PopulateRequest request) {
        int combos = request.categoryCombos() != null ? request.categoryCombos() : 0;
        int catsPerCombo = request.categoriesPerCombo() != null ? request.categoriesPerCombo() : 0;
        int optsPerCat = request.categoryOptionsPerCategory() != null ? request.categoryOptionsPerCategory() : 0;

        if (combos <= 0 || catsPerCombo <= 0 || optsPerCat <= 0) {
            throw new IllegalArgumentException(
                "Category model requires categoryCombos, categoriesPerCombo, and categoryOptionsPerCategory > 0");
        }

        // Validate that option combinations don't exceed the limit
        // Formula: optionsPerCategory ^ categoriesPerCombo
        long optionCombinations = (long) Math.pow(optsPerCat, catsPerCombo);
        if (optionCombinations > MAX_OPTION_COMBINATIONS) {
            throw new IllegalArgumentException(String.format(
                "Option combinations (%d) exceeds maximum allowed (%d). " +
                "%d categories with %d options each results in %d^%d = %d combinations.",
                optionCombinations, MAX_OPTION_COMBINATIONS,
                catsPerCombo, optsPerCat, optsPerCat, catsPerCombo, optionCombinations));
        }

        int totalExpected = request.getTotalCategoryModelCount();
        log.info("Received category model populate request: {} combos, {} cats/combo, {} opts/cat, {} total entities",
            combos, catsPerCombo, optsPerCat, totalExpected);

        PopulateJob job = jobTracker.createJob("categorymodel", totalExpected);
        log.info("Created category model populate job {}", job.getJobId());

        asyncJobExecutor.executeCategoryModelJobAsync(job.getJobId(), combos, catsPerCombo, optsPerCat);

        return job;
    }

    private PopulateJob startDataSetElementJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("DataSetElement requires a non-empty 'categoryComboIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("DataSetElement requires 'amount' > 0");
        }

        int totalExpected = request.amount() * 3; // dataelements + datasets + join rows
        log.info("Received datasetelement populate request: {} dataelements + {} datasets + {} join rows = {} total",
            request.amount(), request.amount(), request.amount(), totalExpected);

        PopulateJob job = jobTracker.createJob("datasetelement", totalExpected);
        log.info("Created datasetelement populate job {}", job.getJobId());

        asyncJobExecutor.executeDataSetElementJobAsync(job.getJobId(), request.amount(), request.categoryComboIds());

        return job;
    }

    private PopulateJob startDataElementJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("Data element requires a non-empty 'categoryComboIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Data element requires 'amount' > 0");
        }

        String valueType = request.resolvedValueType();
        String domainType = request.resolvedDomainType();
        String aggregationType = request.resolvedAggregationType();

        log.info("Received dataelement populate request: {} rows with {} categorycomboid values, valueType={}, domainType={}, aggregationType={}",
            request.amount(), request.categoryComboIds().size(), valueType, domainType, aggregationType);

        PopulateJob job = jobTracker.createJob("dataelement", request.amount());
        log.info("Created dataelement populate job {}", job.getJobId());

        asyncJobExecutor.executeDataElementJobAsync(job.getJobId(), request.amount(),
            request.categoryComboIds(), valueType, domainType, aggregationType);

        return job;
    }

    private PopulateJob startDataSetJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("Dataset requires a non-empty 'categoryComboIds' array");
        }

        if (!request.hasPeriodTypeIds()) {
            throw new IllegalArgumentException("Dataset requires a non-empty 'periodTypeIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Dataset requires 'amount' > 0");
        }

        log.info("Received dataset populate request: {} rows with {} categorycomboid values, {} periodTypeIds",
            request.amount(), request.categoryComboIds().size(), request.periodTypeIds().size());

        PopulateJob job = jobTracker.createJob("dataset", request.amount());
        log.info("Created dataset populate job {}", job.getJobId());

        asyncJobExecutor.executeDataSetJobAsync(job.getJobId(), request.amount(),
            request.categoryComboIds(), request.periodTypeIds());

        return job;
    }

    private PopulateJob startFlatJob(String tableName, int amount) {
        log.info("Received flat populate request for table: {}, amount: {}", tableName, amount);

        PopulateJob job = jobTracker.createJob(tableName, amount);
        log.info("Created populate job {} for table: {}", job.getJobId(), tableName);

        asyncJobExecutor.executeJobAsync(job.getJobId(), tableName, amount);

        return job;
    }

    private PopulateJob startHierarchyJob(String tableName, List<Integer> hierarchy, Long orgunitgroupid) {
        int totalExpected = calculateHierarchyTotal(hierarchy);
        log.info("Received hierarchy populate request for table: {}, hierarchy: {}, expected total: {}",
            tableName, hierarchy, totalExpected);

        // For now, assume parent column is named "parent" - could be made configurable
        String parentColumn = "parentid";

        PopulateJob job = jobTracker.createJob(tableName, totalExpected);
        log.info("Created hierarchy populate job {} for table: {}", job.getJobId(), tableName);

        asyncJobExecutor.executeHierarchyJobAsync(job.getJobId(), tableName, parentColumn, hierarchy, orgunitgroupid);

        return job;
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
}
