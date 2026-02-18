# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DB Populator is a Java 17 Spring Boot service that populates a DHIS2 PostgreSQL database with generated entries. It provides an async API for bulk data insertion with dependency-aware table population.

## Build & Run Commands

```bash
./mvnw clean package              # Build
./mvnw compile                    # Compile only
java -jar target/db-populator.jar # Run
./mvnw test                       # All tests
./mvnw test -Dtest=ClassName#methodName  # Single test
```

## Project Structure

```
src/main/java/com/dbpopulator/
├── controller/
│   └── PopulatorController.java      # REST endpoints
├── service/
│   ├── PopulatorService.java         # Request routing & job creation
│   ├── AsyncJobExecutor.java         # Async task execution
│   ├── BatchInsertService.java       # Generic JDBC batch inserts
│   ├── DataGeneratorService.java     # Random data generation
│   ├── DependencyResolver.java       # Table dependency resolution
│   ├── SchemaDetectionService.java   # DB metadata detection
│   ├── PrimaryKeyGeneratorService.java
│   │
│   │ # Specialized insert services (one per populate type):
│   ├── HierarchyInsertService.java        # organisationunit hierarchy
│   ├── CategoryModelInsertService.java    # categorymodel (combo/cat/opt)
│   └── CategoryDimensionInsertService.java # categorydimension
├── job/
│   ├── PopulateJob.java              # Job state container
│   └── JobTracker.java               # In-memory job management
├── model/
│   ├── PopulateRequest.java          # Request DTO (add fields here)
│   ├── PopulateResponse.java         # Response DTO
│   ├── JobStatus.java                # Job status record
│   ├── TableMetadata.java            # Schema metadata
│   └── ColumnMetadata.java           # Column metadata
└── DbPopulatorApplication.java
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/populate` | POST | Start async populate job |
| `/api/status/{jobId}` | GET | Get job progress |
| `/api/jobs` | GET | List all jobs |
| `/api/jobs/running` | GET | List running jobs |
| `/api/jobs/{jobId}` | DELETE | Remove job |
| `/api/tables` | GET | List all detected tables |
| `/api/tables/{name}` | GET | Get table metadata |
| `/api/schema/refresh` | POST | Re-detect schema |

## Populate Types

### 1. Flat Insert (generic tables)
```json
{"type": "tablename", "amount": 1000}
```
Uses `BatchInsertService` with automatic dependency resolution.

### 2. Hierarchy Insert (organisationunit)
```json
{"type": "organisationunit", "hierarchy": [5, 4, 3], "orgunitgroupid": 123}
```
Creates tree structure: 5 roots -> 20 children -> 60 grandchildren.

### 3. Category Model
```json
{"type": "categorymodel", "categoryCombos": 2, "categoriesPerCombo": 3, "categoryOptionsPerCategory": 4}
```
Creates combos, categories, options, and join table entries.

### 4. Category Dimension
```json
{"type": "categorydimension", "categories": [1, 2, 3], "amount": 1000}
```
Inserts rows with auto-incrementing PK (from max+1) and randomly selected categoryid.

## Adding a New Populate Type

Follow this pattern when implementing a new specialized populate type:

### Step 1: Update PopulateRequest.java
Add new fields and helper methods:
```java
public record PopulateRequest(
    // ... existing fields ...
    List<Long> newFieldIds  // Add new field
) {
    public boolean isNewType() {
        return "newtypename".equalsIgnoreCase(type);
    }

    public boolean hasNewFieldIds() {
        return newFieldIds != null && !newFieldIds.isEmpty();
    }
}
```

### Step 2: Create Insert Service
Create `src/main/java/com/dbpopulator/service/NewTypeInsertService.java`:
```java
@Service
public class NewTypeInsertService {
    private static final int BATCH_SIZE = 1000;
    private final DataSource dataSource;

    public NewTypeInsertService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int insertNewType(int amount, List<Long> ids, ProgressCallback callback) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // ... batch insert logic with commit per batch ...
            // Call callback.onProgress(inserted) after each batch
        }
        return inserted;
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
```

### Step 3: Update PopulatorService.java
Add routing in `startPopulateJob()`:
```java
if (request.isNewType()) {
    return startNewTypeJob(request);
}
```

Add job creation method:
```java
private PopulateJob startNewTypeJob(PopulateRequest request) {
    // Validate request
    if (!request.hasNewFieldIds()) {
        throw new IllegalArgumentException("...");
    }

    PopulateJob job = jobTracker.createJob("newtypename", request.amount());
    asyncJobExecutor.executeNewTypeJobAsync(job.getJobId(), request.amount(), request.newFieldIds());
    return job;
}
```

### Step 4: Update AsyncJobExecutor.java
1. Inject the new service in constructor
2. Add async method:
```java
@Async("populatorExecutor")
public void executeNewTypeJobAsync(String jobId, int amount, List<Long> ids) {
    jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
    jobTracker.registerTable(jobId, "newtypename", amount, false);

    try {
        int inserted = newTypeInsertService.insertNewType(amount, ids,
            (totalInserted) -> jobTracker.updateTableProgress(jobId, "newtypename", totalInserted));

        jobTracker.updateTableProgress(jobId, "newtypename", inserted);
        jobTracker.markCompleted(jobId);
    } catch (Exception e) {
        jobTracker.markFailed(jobId, e.getMessage());
    }
}
```

## Key Patterns

### Batch Insert Pattern
```java
try (PreparedStatement ps = conn.prepareStatement(sql)) {
    for (int i = 0; i < amount; i++) {
        ps.setLong(1, value);
        ps.addBatch();

        if ((i + 1) % BATCH_SIZE == 0) {
            ps.executeBatch();
            ps.clearBatch();
            conn.commit();
            callback.onProgress(i + 1);
        }
    }
    // Execute remaining
    if (amount % BATCH_SIZE != 0) {
        ps.executeBatch();
        conn.commit();
    }
}
```

### Get Max PK Pattern
```java
private long getMaxPkValue(Connection conn, String table, String pkColumn) throws SQLException {
    String sql = "SELECT COALESCE(MAX(" + pkColumn + "), 0) FROM " + table;
    try (PreparedStatement ps = conn.prepareStatement(sql);
         ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0;
    }
}
```

### Progress Callback Pattern
All insert services use a functional interface for progress reporting:
```java
@FunctionalInterface
public interface ProgressCallback {
    void onProgress(int totalInserted);
}
```

## Key Services Reference

| Service | Purpose |
|---------|---------|
| `SchemaDetectionService` | Get table metadata, check table existence |
| `DataGeneratorService` | Generate random values by type, FK cache |
| `JobTracker` | Create jobs, update progress, mark complete/failed |
| `BatchInsertService` | Generic table inserts with dependency resolution |

## Configuration

- `app.batch.size` - JDBC batch size (default: 1000)
- Database connection via Spring `spring.datasource.*` properties
- HikariCP connection pooling auto-configured
