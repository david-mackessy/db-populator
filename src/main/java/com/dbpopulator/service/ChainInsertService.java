package com.dbpopulator.service;

import com.dbpopulator.model.PopulateRequest;
import com.dbpopulator.model.PopulateRequest.UserRoleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Executes a chain of populate requests in dependency order, then runs any
 * applicable cross-request mappings (e.g. usermembership for organisationunit + userinfo).
 *
 * Execution priority (lower = runs first):
 *   organisationunit(10) → orgunitgroup(15) → categorymodel(20) → categorydimension(25)
 *   → dataelement/dataset/datasetelement/program/programindicator/dataapprovalworkflow(30-55)
 *   → userrole(60) → usergroup(65) → userinfo(70)
 *
 * Cross-mappings applied after all sub-requests complete:
 *   - organisationunit hierarchy + userinfo → usermembership
 *     Each user gets 1 org unit per hierarchy level (randomly selected from that level's units).
 */
@Service
public class ChainInsertService {

    private static final Logger log = LoggerFactory.getLogger(ChainInsertService.class);
    private static final int BATCH_SIZE = 1000;

    private static final Map<String, Integer> TYPE_PRIORITY = Map.ofEntries(
        Map.entry("organisationunit", 10),
        Map.entry("orgunitgroup", 15),
        Map.entry("categorymodel", 20),
        Map.entry("categorydimension", 25),
        Map.entry("dataelement", 30),
        Map.entry("dataset", 35),
        Map.entry("datasetelement", 40),
        Map.entry("program", 45),
        Map.entry("programindicator", 50),
        Map.entry("dataapprovalworkflow", 55),
        Map.entry("userrole", 60),
        Map.entry("usergroup", 65),
        Map.entry("userinfo", 70)
    );
    private static final int DEFAULT_PRIORITY = 50;

    private final HierarchyInsertService hierarchyInsertService;
    private final CategoryModelInsertService categoryModelInsertService;
    private final CategoryDimensionInsertService categoryDimensionInsertService;
    private final DataElementInsertService dataElementInsertService;
    private final DataSetInsertService dataSetInsertService;
    private final DataSetElementInsertService dataSetElementInsertService;
    private final ProgramInsertService programInsertService;
    private final ProgramIndicatorInsertService programIndicatorInsertService;
    private final DataApprovalWorkflowInsertService dataApprovalWorkflowInsertService;
    private final UserRoleInsertService userRoleInsertService;
    private final UserGroupInsertService userGroupInsertService;
    private final UserInfoInsertService userInfoInsertService;
    private final OrgUnitGroupInsertService orgUnitGroupInsertService;
    private final DataSource dataSource;

    public ChainInsertService(HierarchyInsertService hierarchyInsertService,
                               CategoryModelInsertService categoryModelInsertService,
                               CategoryDimensionInsertService categoryDimensionInsertService,
                               DataElementInsertService dataElementInsertService,
                               DataSetInsertService dataSetInsertService,
                               DataSetElementInsertService dataSetElementInsertService,
                               ProgramInsertService programInsertService,
                               ProgramIndicatorInsertService programIndicatorInsertService,
                               DataApprovalWorkflowInsertService dataApprovalWorkflowInsertService,
                               UserRoleInsertService userRoleInsertService,
                               UserGroupInsertService userGroupInsertService,
                               UserInfoInsertService userInfoInsertService,
                               OrgUnitGroupInsertService orgUnitGroupInsertService,
                               DataSource dataSource) {
        this.hierarchyInsertService = hierarchyInsertService;
        this.categoryModelInsertService = categoryModelInsertService;
        this.categoryDimensionInsertService = categoryDimensionInsertService;
        this.dataElementInsertService = dataElementInsertService;
        this.dataSetInsertService = dataSetInsertService;
        this.dataSetElementInsertService = dataSetElementInsertService;
        this.programInsertService = programInsertService;
        this.programIndicatorInsertService = programIndicatorInsertService;
        this.dataApprovalWorkflowInsertService = dataApprovalWorkflowInsertService;
        this.userRoleInsertService = userRoleInsertService;
        this.userGroupInsertService = userGroupInsertService;
        this.userInfoInsertService = userInfoInsertService;
        this.orgUnitGroupInsertService = orgUnitGroupInsertService;
        this.dataSource = dataSource;
    }

    public int executeChain(List<PopulateRequest> requests, ProgressCallback callback) throws Exception {
        List<PopulateRequest> ordered = requests.stream()
            .sorted(Comparator.comparingInt(r -> TYPE_PRIORITY.getOrDefault(
                r.type() != null ? r.type().toLowerCase() : "", DEFAULT_PRIORITY)))
            .toList();

        log.info("Chain execution order: {}", ordered.stream().map(PopulateRequest::type).toList());

        HierarchyInsertService.HierarchyResult hierarchyResult = null;
        UserInfoInsertService.UserInsertResult userInsertResult = null;
        int total = 0;

        for (PopulateRequest req : ordered) {
            final int offset = total;
            String type = req.type() != null ? req.type().toLowerCase() : "";

            log.info("Chain: executing sub-request type='{}'", req.type());

            if ("organisationunit".equals(type) && req.hasHierarchy()) {
                HierarchyInsertService.HierarchyResult result = hierarchyInsertService.insertHierarchy(
                    "organisationunit", "parentid", req.hierarchy(), req.orgunitgroupid(),
                    (t) -> callback.onProgress(offset + t));
                hierarchyResult = result;
                total += result.totalInserted();

            } else if (req.isUserInfo()) {
                int roles = req.amountRoles() != null ? req.amountRoles() : 0;
                int groups = req.amountUserGroups() != null ? req.amountUserGroups() : 0;
                if (req.amount() <= 0) throw new IllegalArgumentException("userinfo requires 'amount' > 0");
                if (roles <= 0) throw new IllegalArgumentException("userinfo requires 'amountRoles' > 0");
                if (groups <= 0) throw new IllegalArgumentException("userinfo requires 'amountUserGroups' > 0");
                UserInfoInsertService.UserInsertResult result = userInfoInsertService.insertUsers(
                    req.amount(), roles, groups, (t) -> callback.onProgress(offset + t));
                userInsertResult = result;
                total += result.totalInserted();

            } else if (req.isCategoryModel()) {
                int combos = req.categoryCombos() != null ? req.categoryCombos() : 0;
                int catsPerCombo = req.categoriesPerCombo() != null ? req.categoriesPerCombo() : 0;
                int optsPerCat = req.categoryOptionsPerCategory() != null ? req.categoryOptionsPerCategory() : 0;
                if (combos <= 0 || catsPerCombo <= 0 || optsPerCat <= 0)
                    throw new IllegalArgumentException("categorymodel requires categoryCombos, categoriesPerCombo, categoryOptionsPerCategory > 0");
                total += categoryModelInsertService.insertCategoryModel(combos, catsPerCombo, optsPerCat,
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isCategoryDimension()) {
                if (!req.hasCategories()) throw new IllegalArgumentException("categorydimension requires 'categories' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("categorydimension requires 'amount' > 0");
                total += categoryDimensionInsertService.insertCategoryDimensions(req.amount(), req.categories(),
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isDataElement()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("dataelement requires 'categoryComboIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("dataelement requires 'amount' > 0");
                total += dataElementInsertService.insertDataElements(req.amount(), req.categoryComboIds(),
                    req.resolvedValueType(), req.resolvedDomainType(), req.resolvedAggregationType(),
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isDataSet()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("dataset requires 'categoryComboIds' array");
                if (!req.hasPeriodTypeIds()) throw new IllegalArgumentException("dataset requires 'periodTypeIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("dataset requires 'amount' > 0");
                total += dataSetInsertService.insertDataSets(req.amount(), req.categoryComboIds(),
                    req.periodTypeIds(), (t) -> callback.onProgress(offset + t));

            } else if (req.isDataSetElement()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("datasetelement requires 'categoryComboIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("datasetelement requires 'amount' > 0");
                total += dataSetElementInsertService.insertDataSetElements(req.amount(), req.categoryComboIds(),
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isProgram()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("program requires 'categoryComboIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("program requires 'amount' > 0");
                total += programInsertService.insertPrograms(req.amount(), req.categoryComboIds(),
                    req.resolvedProgramType(), (t) -> callback.onProgress(offset + t));

            } else if (req.isProgramIndicator()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("programindicator requires 'categoryComboIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("programindicator requires 'amount' > 0");
                total += programIndicatorInsertService.insertProgramIndicators(req.amount(), req.categoryComboIds(),
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isDataApprovalWorkflow()) {
                if (!req.hasCategoryComboIds()) throw new IllegalArgumentException("dataapprovalworkflow requires 'categoryComboIds' array");
                if (req.amount() <= 0) throw new IllegalArgumentException("dataapprovalworkflow requires 'amount' > 0");
                total += dataApprovalWorkflowInsertService.insertDataApprovalWorkflows(req.amount(), req.categoryComboIds(),
                    (t) -> callback.onProgress(offset + t));

            } else if (req.isUserRole()) {
                List<UserRoleEntry> entries = req.hasUserRoles()
                    ? req.userRoles()
                    : generateRandomUserRoleEntries(req.amount());
                if (entries.isEmpty()) throw new IllegalArgumentException("userrole requires 'userRoles' array or 'amount' > 0");
                total += userRoleInsertService.insertUserRoles(entries, (t) -> callback.onProgress(offset + t));

            } else if (req.isUserGroup()) {
                if (req.amount() <= 0) throw new IllegalArgumentException("usergroup requires 'amount' > 0");
                total += userGroupInsertService.insertUserGroups(req.amount(), (t) -> callback.onProgress(offset + t));

            } else if (req.isOrgUnitGroup()) {
                if (req.amount() <= 0) throw new IllegalArgumentException("orgunitgroup requires 'amount' > 0");
                total += orgUnitGroupInsertService.insertOrgUnitGroups(req.amount(), (t) -> callback.onProgress(offset + t));

            } else {
                throw new IllegalArgumentException("Unsupported type in chain: '" + req.type() + "'");
            }

            log.info("Chain: sub-request '{}' complete, running total={}", req.type(), total);
        }

        // Cross-mapping: usermembership (org unit hierarchy + userinfo)
        if (hierarchyResult != null && userInsertResult != null
                && !hierarchyResult.orgUnitsByLevel().isEmpty()) {
            log.info("Chain: inserting usermembership — {} users × {} hierarchy levels",
                userInsertResult.userCount(), hierarchyResult.orgUnitsByLevel().size());
            int memberships = insertUserMemberships(hierarchyResult.orgUnitsByLevel(), userInsertResult, callback, total);
            total += memberships;
            log.info("Chain: usermembership complete, {} rows inserted", memberships);
        }

        return total;
    }

    /**
     * Inserts one usermembership row per user per hierarchy level.
     * Each user is assigned a randomly selected org unit from each level.
     */
    private int insertUserMemberships(List<List<Object>> orgUnitsByLevel,
                                       UserInfoInsertService.UserInsertResult userResult,
                                       ProgressCallback callback, int baseTotal) throws SQLException {
        String sql = "INSERT INTO usermembership (userinfoid, organisationunitid) VALUES (?, ?)";
        int inserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < userResult.userCount(); i++) {
                    long userId = userResult.firstUserId() + i;
                    for (List<Object> levelIds : orgUnitsByLevel) {
                        if (levelIds.isEmpty()) continue;
                        Object orgUnitId = levelIds.get(ThreadLocalRandom.current().nextInt(levelIds.size()));
                        ps.setLong(1, userId);
                        ps.setLong(2, ((Number) orgUnitId).longValue());
                        ps.addBatch();
                        inserted++;

                        if (inserted % BATCH_SIZE == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                            conn.commit();
                            if (callback != null) callback.onProgress(baseTotal + inserted);
                        }
                    }
                }

                if (inserted % BATCH_SIZE != 0) {
                    ps.executeBatch();
                    conn.commit();
                    if (callback != null) callback.onProgress(baseTotal + inserted);
                }

            } catch (SQLException e) {
                conn.rollback();
                log.error("usermembership insert failed, rolling back", e);
                throw e;
            }
        }

        return inserted;
    }

    private List<UserRoleEntry> generateRandomUserRoleEntries(int count) {
        List<UserRoleEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = "Role-" + UUID.randomUUID().toString().substring(0, 8);
            entries.add(new UserRoleEntry(name, name));
        }
        return entries;
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
