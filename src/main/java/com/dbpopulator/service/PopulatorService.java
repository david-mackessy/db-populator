package com.dbpopulator.service;

import com.dbpopulator.job.JobTracker;
import com.dbpopulator.job.PopulateJob;
import com.dbpopulator.model.PopulateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.dbpopulator.model.PopulateRequest.UserRoleEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

        if (request.isProgram()) {
            return startProgramJob(request);
        }

        if (request.isProgramIndicator()) {
            return startProgramIndicatorJob(request);
        }

        if (request.isDataApprovalWorkflow()) {
            return startDataApprovalWorkflowJob(request);
        }

        if (request.isUserRole()) {
            return startUserRoleJob(request);
        }

        if (request.isUserGroup()) {
            return startUserGroupJob(request);
        }

        if (request.isUserInfo()) {
            return startUserInfoJob(request);
        }

        if (request.isOrgUnitGroup()) {
            return startOrgUnitGroupJob(request);
        }

        if (request.isChain()) {
            return startChainJob(request);
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

    private PopulateJob startProgramJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("Program requires a non-empty 'categoryComboIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Program requires 'amount' > 0");
        }

        String programType = request.resolvedProgramType();

        log.info("Received program populate request: {} rows with {} categorycomboid values, programType={}",
            request.amount(), request.categoryComboIds().size(), programType);

        PopulateJob job = jobTracker.createJob("program", request.amount());
        log.info("Created program populate job {}", job.getJobId());

        asyncJobExecutor.executeProgramJobAsync(job.getJobId(), request.amount(),
            request.categoryComboIds(), programType);

        return job;
    }

    private PopulateJob startProgramIndicatorJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("Program indicator requires a non-empty 'categoryComboIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Program indicator requires 'amount' > 0");
        }

        int totalExpected = 10 + request.amount(); // 10 programs + amount indicators
        log.info("Received programindicator populate request: 10 programs + {} indicators = {} total",
            request.amount(), totalExpected);

        PopulateJob job = jobTracker.createJob("programindicator", totalExpected);
        log.info("Created programindicator populate job {}", job.getJobId());

        asyncJobExecutor.executeProgramIndicatorJobAsync(job.getJobId(), request.amount(), request.categoryComboIds());

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

    private PopulateJob startDataApprovalWorkflowJob(PopulateRequest request) {
        if (!request.hasCategoryComboIds()) {
            throw new IllegalArgumentException("Data approval workflow requires a non-empty 'categoryComboIds' array");
        }

        if (request.amount() <= 0) {
            throw new IllegalArgumentException("Data approval workflow requires 'amount' > 0");
        }

        log.info("Received dataapprovalworkflow populate request: {} rows with {} categoryComboId values",
            request.amount(), request.categoryComboIds().size());

        PopulateJob job = jobTracker.createJob("dataapprovalworkflow", request.amount());
        log.info("Created dataapprovalworkflow populate job {}", job.getJobId());

        asyncJobExecutor.executeDataApprovalWorkflowJobAsync(job.getJobId(), request.amount(), request.categoryComboIds());

        return job;
    }

    private PopulateJob startUserRoleJob(PopulateRequest request) {
        List<UserRoleEntry> entries;

        if (request.hasUserRoles()) {
            entries = request.userRoles();
            log.info("Received userrole populate request: {} explicit role-authority pairs", entries.size());
        } else if (request.amount() > 0) {
            entries = generateRandomUserRoleEntries(request.amount());
            log.info("Received userrole populate request: {} randomly generated roles", entries.size());
        } else {
            throw new IllegalArgumentException(
                "userrole requires either a non-empty 'userRoles' array or 'amount' > 0");
        }

        int totalExpected = entries.size() * 2; // N userroles + N userroleauthorities
        PopulateJob job = jobTracker.createJob("userrole", totalExpected);
        log.info("Created userrole populate job {}", job.getJobId());

        asyncJobExecutor.executeUserRoleJobAsync(job.getJobId(), entries);

        return job;
    }

    private PopulateJob startUserInfoJob(PopulateRequest request) {
        if (request.amount() <= 0) {
            throw new IllegalArgumentException("userinfo requires 'amount' > 0");
        }
        int roles = request.amountRoles() != null ? request.amountRoles() : 0;
        int groups = request.amountUserGroups() != null ? request.amountUserGroups() : 0;
        if (roles <= 0) {
            throw new IllegalArgumentException("userinfo requires 'amountRoles' > 0");
        }
        if (groups <= 0) {
            throw new IllegalArgumentException("userinfo requires 'amountUserGroups' > 0");
        }

        // userrole + userroleauthorities + usergroup + userinfo + userrolemembers + usergroupmembers
        int totalExpected = roles * 2 + groups + request.amount() * 3;
        log.info("Received userinfo populate request: {} users, {} roles, {} groups ({} total rows)",
            request.amount(), roles, groups, totalExpected);

        PopulateJob job = jobTracker.createJob("userinfo", totalExpected);
        log.info("Created userinfo populate job {}", job.getJobId());

        asyncJobExecutor.executeUserInfoJobAsync(job.getJobId(), request.amount(), roles, groups);

        return job;
    }

    private PopulateJob startOrgUnitGroupJob(PopulateRequest request) {
        if (request.amount() <= 0) {
            throw new IllegalArgumentException("orgunitgroup requires 'amount' > 0");
        }

        log.info("Received orgunitgroup populate request: {} rows", request.amount());

        PopulateJob job = jobTracker.createJob("orgunitgroup", request.amount());
        log.info("Created orgunitgroup populate job {}", job.getJobId());

        asyncJobExecutor.executeOrgUnitGroupJobAsync(job.getJobId(), request.amount());

        return job;
    }

    private PopulateJob startUserGroupJob(PopulateRequest request) {
        if (request.amount() <= 0) {
            throw new IllegalArgumentException("usergroup requires 'amount' > 0");
        }

        log.info("Received usergroup populate request: {} rows", request.amount());

        PopulateJob job = jobTracker.createJob("usergroup", request.amount());
        log.info("Created usergroup populate job {}", job.getJobId());

        asyncJobExecutor.executeUserGroupJobAsync(job.getJobId(), request.amount());

        return job;
    }

    private List<UserRoleEntry> generateRandomUserRoleEntries(int count) {
        List<UserRoleEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = "Role-" + UUID.randomUUID().toString().substring(0, 8);
            entries.add(new UserRoleEntry(name, name));
        }
        return entries;
    }

    private PopulateJob startChainJob(PopulateRequest request) {
        if (!request.hasRequests()) {
            throw new IllegalArgumentException("chain requires a non-empty 'requests' array");
        }

        int totalExpected = estimateChainTotal(request.requests());
        log.info("Received chain populate request: {} sub-requests, ~{} total expected rows",
            request.requests().size(), totalExpected);

        PopulateJob job = jobTracker.createJob("chain", totalExpected);
        log.info("Created chain populate job {}", job.getJobId());

        asyncJobExecutor.executeChainJobAsync(job.getJobId(), request.requests(), totalExpected);

        return job;
    }

    private int estimateChainTotal(List<PopulateRequest> requests) {
        int total = 0;
        for (PopulateRequest req : requests) {
            if (req.hasHierarchy()) {
                total += req.getTotalHierarchyCount();
            } else if (req.isCategoryModel()) {
                total += req.getTotalCategoryModelCount();
            } else if (req.isUserInfo()) {
                int roles = req.amountRoles() != null ? req.amountRoles() : 0;
                int groups = req.amountUserGroups() != null ? req.amountUserGroups() : 0;
                total += roles * 2 + groups + req.amount() * 3;
            } else if (req.isUserRole()) {
                int count = req.hasUserRoles() ? req.userRoles().size() : req.amount();
                total += count * 2;
            } else {
                total += req.amount();
            }
        }
        return total;
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
