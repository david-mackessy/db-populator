Add a new populate request type for the DB Populator service. The request type is: $ARGUMENTS

Follow these steps in order. Do NOT skip steps or make assumptions — ask the user clarifying questions where indicated.

---

## Step 1: Discover the target table

The primary table to populate is the one matching the type argument (e.g. type `indicator` → table `indicator`).

1. Call `GET /api/tables/<type>` mentally via `schemaService.getTable("<type>")` — but since you're in code context, read `SchemaDetectionService.java` to understand the table lookup mechanism.
2. Read the actual DB table structure by checking `GET /api/tables/<type>` endpoint docs or, better, look at how existing services like `DataElementInsertService.java` query the schema.
3. **Ask the user**: "Is the primary table name exactly `<type>` or is it different?" (e.g. `userrole` vs `userroles`). Confirm before proceeding.

---

## Step 2: Discover required columns

1. Read `SchemaDetectionService.java` to understand how `getInsertableColumns()` works — it returns non-nullable columns that must be set.
2. Read `DataGeneratorService.java` to understand what `generateRow(table)` auto-fills (random strings, numbers, UUIDs, FK lookups, etc.).
3. From this, determine which columns `generateRow()` CANNOT fill automatically:
   - Enum columns with a fixed allowed set (e.g. `valuetype`, `domaintype`)
   - FK columns pointing to tables where IDs must be provided by the caller (not randomly selected from existing rows)
   - Columns with business-logic constraints (e.g. must match a pattern, must be unique in a specific way)
4. **Ask the user** about each ambiguous column:
   - "Column `<col>` is NOT NULL and has type `<type>`. Should callers provide this value, or should we pick randomly from existing rows / use a default?"
   - For enum columns: "What are the valid values for `<col>`? Should we default to one or let the caller choose?"

---

## Step 3: Discover related tables

Identify tables that should be populated as part of this request type. Look at similar services for the pattern:

- **Join/mapping tables**: e.g. `userroleauthorities` is always inserted alongside `userrole`. Look for tables with FKs pointing back to the primary table.
- **Child tables**: e.g. `programindicator` creates programs first, then indicators.
- **Required parent data**: e.g. `dataelement` requires a `categorycomboid` — but this is provided by the caller, not auto-created.

**Ask the user**:
- "Should inserting `<type>` also populate any related tables automatically? For example, are there join tables or child records that should always be created together?"
- "Are there any parent records that callers must supply as IDs (like `categoryComboIds`), or should we create them automatically?"

---

## Step 4: Design the request shape

Based on Steps 2–3, propose a JSON request body to the user:

```json
{
  "type": "<type>",
  "amount": 100,
  // any caller-supplied IDs or enums discovered above
}
```

**Ask the user to confirm** the shape before writing any code.

---

## Step 5: Implement — PopulateRequest.java

Read `src/main/java/com/dbpopulator/model/PopulateRequest.java`.

Add:
- New fields to the record for any caller-supplied parameters (IDs, enums, counts)
- `isXxx()` method: `return "<type>".equalsIgnoreCase(type);`
- `hasXxx()` helpers for list fields: `return xxxIds != null && !xxxIds.isEmpty();`
- If enum fields are added, add a `resolvedXxx()` method that validates and defaults the value (follow the pattern of `resolvedValueType()`)

---

## Step 6: Implement — XxxInsertService.java

Create `src/main/java/com/dbpopulator/service/<TypePascalCase>InsertService.java`.

Rules:
1. Inject `DataSource`, `SchemaDetectionService`, and `DataGeneratorService`
2. Use `schemaService.getTable("<type>")` to get `TableMetadata` — never hardcode column lists
3. Use `dataGenerator.generateRow(table)` to auto-fill all required columns, then `row.put(...)` to override specific fields
4. Use the max-PK pattern for IDs:
   ```java
   long nextId = getMaxPkValue(conn) + 1;
   ```
   Never use `RETURNING` or `Statement.RETURN_GENERATED_KEYS`
5. Use batch inserts with `BATCH_SIZE = 1000`, commit per batch, call `callback.onProgress(inserted)` after each commit
6. For related tables (join tables, child rows), insert them in the same transaction block or immediately after the primary rows

Follow the structure of `DataElementInsertService.java` or `UserRoleInsertService.java` as a reference.

---

## Step 7: Implement — PopulatorService.java

Read `src/main/java/com/dbpopulator/service/PopulatorService.java`.

1. Add routing at the top of `startPopulateJob()`:
   ```java
   if (request.isXxx()) {
       return startXxxJob(request);
   }
   ```
2. Add a `startXxxJob(PopulateRequest request)` method that:
   - Validates required fields (throw `IllegalArgumentException` with a clear message if missing)
   - Calculates `totalExpected` rows (sum of primary + related table rows)
   - Creates the job: `jobTracker.createJob("<type>", totalExpected)`
   - Calls `asyncJobExecutor.executeXxxJobAsync(...)`
   - Returns the job

---

## Step 8: Implement — AsyncJobExecutor.java

Read `src/main/java/com/dbpopulator/service/AsyncJobExecutor.java`.

1. Inject the new `XxxInsertService` in the constructor
2. Add an `@Async("populatorExecutor")` method:
   ```java
   @Async("populatorExecutor")
   public void executeXxxJobAsync(String jobId, int amount, /* other params */) {
       jobTracker.updateJobStatus(jobId, JobStatus.Status.RUNNING);
       jobTracker.registerTable(jobId, "<type>", amount, false);
       try {
           int inserted = xxxInsertService.insertXxx(amount, ...,
               (totalInserted) -> jobTracker.updateTableProgress(jobId, "<type>", totalInserted));
           jobTracker.updateTableProgress(jobId, "<type>", inserted);
           jobTracker.markCompleted(jobId);
       } catch (Exception e) {
           jobTracker.markFailed(jobId, e.getMessage());
       }
   }
   ```

---

## Step 9: Implement — PopulatorController.java

Read `src/main/java/com/dbpopulator/controller/PopulatorController.java`.

Add `request.isXxx()` to the `isSpecializedType` boolean **only if** the new type:
- Does not require `amount > 0` to be meaningful, OR
- Does not correspond to a real schema table (i.e. it's a compound/virtual type)

If the type maps directly to a real table and always requires `amount`, it may NOT need to be added to `isSpecializedType` — the generic flat-insert path will handle the table check automatically.

---

## Step 10: Test

Build and verify:
```bash
./mvnw compile
```

Then test the endpoint:
```bash
curl -X POST http://localhost:8080/api/populate \
  -H "Content-Type: application/json" \
  -d '{"type": "<type>", "amount": 5, ...}'
```

Poll the returned `statusUrl` until the job completes. Check that:
- The job reaches `COMPLETED` status
- Row counts in the target table(s) increased by the expected amount
- No FK constraint violations or NULL constraint errors in the logs
