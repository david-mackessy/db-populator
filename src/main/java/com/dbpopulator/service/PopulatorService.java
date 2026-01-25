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

    private PopulateJob startCategoryModelJob(PopulateRequest request) {
        int combos = request.categoryCombos() != null ? request.categoryCombos() : 0;
        int catsPerCombo = request.categoriesPerCombo() != null ? request.categoriesPerCombo() : 0;
        int optsPerCat = request.categoryOptionsPerCategory() != null ? request.categoryOptionsPerCategory() : 0;

        if (combos <= 0 || catsPerCombo <= 0 || optsPerCat <= 0) {
            throw new IllegalArgumentException(
                "Category model requires categoryCombos, categoriesPerCombo, and categoryOptionsPerCategory > 0");
        }

        int totalExpected = request.getTotalCategoryModelCount();
        log.info("Received category model populate request: {} combos, {} cats/combo, {} opts/cat, {} total entities",
            combos, catsPerCombo, optsPerCat, totalExpected);

        PopulateJob job = jobTracker.createJob("categorymodel", totalExpected);
        log.info("Created category model populate job {}", job.getJobId());

        asyncJobExecutor.executeCategoryModelJobAsync(job.getJobId(), combos, catsPerCombo, optsPerCat);

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
