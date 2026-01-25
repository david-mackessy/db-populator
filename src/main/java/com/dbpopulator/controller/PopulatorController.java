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
        if (request.type() == null || request.type().isBlank()) {
            return ResponseEntity.badRequest().build();
        }

        // Handle category model requests separately
        if (request.isCategoryModel()) {
            if (request.categoryCombos() == null || request.categoryCombos() <= 0 ||
                request.categoriesPerCombo() == null || request.categoriesPerCombo() <= 0 ||
                request.categoryOptionsPerCategory() == null || request.categoryOptionsPerCategory() <= 0) {
                return ResponseEntity.badRequest().build();
            }

            PopulateJob job = populatorService.startPopulateJob(request);

            String statusUrl = ServletUriComponentsBuilder.fromCurrentContextPath()
                .path("/api/status/{jobId}")
                .buildAndExpand(job.getJobId())
                .toUriString();

            return ResponseEntity.accepted()
                .body(new PopulateResponse(job.getJobId(), statusUrl));
        }

        String tableName = request.type();

        // Hierarchy is only valid for organisationunit
        if (request.hasHierarchy() && !"organisationunit".equalsIgnoreCase(tableName)) {
            return ResponseEntity.badRequest().build();
        }

        // Either amount or hierarchy must be provided
        if (!request.hasHierarchy() && request.amount() <= 0) {
            return ResponseEntity.badRequest().build();
        }

        if (!schemaService.tableExists(tableName)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new PopulateResponse(null, null));
        }

        PopulateJob job = populatorService.startPopulateJob(request);

        String statusUrl = ServletUriComponentsBuilder.fromCurrentContextPath()
            .path("/api/status/{jobId}")
            .buildAndExpand(job.getJobId())
            .toUriString();

        return ResponseEntity.accepted()
            .body(new PopulateResponse(job.getJobId(), statusUrl));
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
