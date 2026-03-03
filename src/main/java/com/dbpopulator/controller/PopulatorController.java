package com.dbpopulator.controller;

import com.dbpopulator.job.JobTracker;
import com.dbpopulator.job.PopulateJob;
import com.dbpopulator.model.JobStatus;
import com.dbpopulator.model.PopulateRequest;
import com.dbpopulator.model.PopulateResponse;
import com.dbpopulator.model.TableInfo;
import com.dbpopulator.model.TableMetadata;
import com.dbpopulator.service.PopulatorService;
import com.dbpopulator.service.SchemaDetectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class PopulatorController {

    private static final Logger log = LoggerFactory.getLogger(PopulatorController.class);

    private final PopulatorService populatorService;
    private final JobTracker jobTracker;
    private final SchemaDetectionService schemaService;

    public PopulatorController(PopulatorService populatorService,
                               JobTracker jobTracker,
                               SchemaDetectionService schemaService) {
        this.populatorService = populatorService;
        this.jobTracker = jobTracker;
        this.schemaService = schemaService;
    }

    @PostMapping("/populate")
    public ResponseEntity<PopulateResponse> populate(@RequestBody PopulateRequest request) {
        log.info("Received populate request: type={}", request.type());

        if (request.type() == null || request.type().isBlank()) {
            log.warn("Populate request rejected: missing 'type' field");
            return badRequest("Missing required field: 'type'");
        }

        // Specialized types manage their own validation in the service layer.
        // They do not require 'amount' or an existing table in the schema.
        boolean isSpecializedType = request.isCategoryModel()
            || request.isCategoryDimension()
            || request.isDataElement()
            || request.isDataSet()
            || request.isDataSetElement()
            || request.isProgram()
            || request.isProgramIndicator()
            || request.isDataApprovalWorkflow()
            || request.isUserRole()
            || request.isUserGroup()
            || request.isUserInfo()
            || request.isOrgUnitGroup()
            || request.isChain();

        if (!isSpecializedType) {
            // Generic flat/hierarchy inserts require amount and a real table
            if (request.hasHierarchy() && !"organisationunit".equalsIgnoreCase(request.type())) {
                log.warn("Populate request rejected: hierarchy is only supported for 'organisationunit', got '{}'", request.type());
                return badRequest("Hierarchy is only supported for type 'organisationunit'");
            }

            if (!request.hasHierarchy() && request.amount() <= 0) {
                log.warn("Populate request rejected: 'amount' must be > 0 for type '{}'", request.type());
                return badRequest("Field 'amount' must be > 0");
            }

            if (!schemaService.tableExists(request.type())) {
                log.warn("Populate request rejected: table '{}' not found in schema", request.type());
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new PopulateResponse(null, null, "Table not found: " + request.type()));
            }
        }

        try {
            PopulateJob job = populatorService.startPopulateJob(request);

            String statusUrl = ServletUriComponentsBuilder.fromCurrentContextPath()
                .path("/api/status/{jobId}")
                .buildAndExpand(job.getJobId())
                .toUriString();

            log.info("Started populate job {} for type '{}'", job.getJobId(), request.type());
            return ResponseEntity.accepted()
                .body(new PopulateResponse(job.getJobId(), statusUrl));

        } catch (IllegalArgumentException e) {
            log.warn("Populate request rejected for type '{}': {}", request.type(), e.getMessage());
            return badRequest(e.getMessage());
        }
    }

    private ResponseEntity<PopulateResponse> badRequest(String message) {
        return ResponseEntity.badRequest().body(new PopulateResponse(null, null, message));
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<JobStatus> getStatus(@PathVariable String jobId) {
        return jobTracker.getJobStatus(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/jobs")
    public ResponseEntity<Collection<JobStatus>> getAllJobs() {
        return ResponseEntity.ok(jobTracker.getAllJobs());
    }

    @GetMapping("/jobs/running")
    public ResponseEntity<Collection<JobStatus>> getRunningJobs() {
        return ResponseEntity.ok(jobTracker.getRunningJobs());
    }

    @DeleteMapping("/jobs/{jobId}")
    public ResponseEntity<Void> deleteJob(@PathVariable String jobId) {
        if (jobTracker.getJob(jobId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        jobTracker.removeJob(jobId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/tables")
    public ResponseEntity<List<TableInfo>> getTables() {
        List<TableInfo> tables = schemaService.getAllTables().stream()
            .map(t -> new TableInfo(t.tableName(), schemaService.getTableCount(t.tableName())))
            .sorted((a, b) -> a.name().compareTo(b.name()))
            .toList();
        return ResponseEntity.ok(tables);
    }

    @GetMapping("/tables/{tableName}")
    public ResponseEntity<TableMetadata> getTable(@PathVariable String tableName) {
        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(table);
    }

    @PostMapping("/schema/refresh")
    public ResponseEntity<Map<String, String>> refreshSchema() {
        try {
            schemaService.refreshSchema();
            return ResponseEntity.ok(Map.of("message", "Schema refreshed successfully"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/schema/auto-generate-pks")
    public ResponseEntity<Map<String, Object>> makeAllPrimaryKeysAutoGenerated() {
        try {
            int updated = schemaService.makeAllPrimaryKeysAutoGenerated();
            return ResponseEntity.ok(Map.of(
                "message", "Primary keys updated successfully",
                "tablesUpdated", updated
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}
