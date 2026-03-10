# DB Populator

100% AI-generated App. 
Solely to insert large amounts of data to a DHS2 DB, for performance testing. 

A Java 17 Spring Boot service for bulk-populating a DHIS2 PostgreSQL database with generated test data. All operations are async — requests return a job ID immediately, and progress can be tracked via the status endpoint.

## Prerequisites

- Java 17+
- A running DHIS2 PostgreSQL database
- Configure `src/main/resources/application.properties` with your database connection

## Build & Run

```bash
./mvnw clean package              # Build jar
java -jar target/db-populator.jar # Run
```

## API Overview

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/populate` | POST | Start an async populate job |
| `/api/status/{jobId}` | GET | Get job progress |
| `/api/jobs` | GET | List all jobs |
| `/api/jobs/running` | GET | List running jobs |
| `/api/jobs/{jobId}` | DELETE | Remove a job |
| `/api/tables` | GET | List all detected DB tables |
| `/api/tables/{name}` | GET | Get table metadata (columns, nullability) |
| `/api/schema/refresh` | POST | Re-detect schema from DB |
| `/api/health` | GET | Health check |

All populate requests return:
```json
{ "jobId": "abc-123", "statusUrl": "http://host/api/status/abc-123" }
```

Poll the `statusUrl` to track progress.

---

## Available Request Types

### 1. Generic Flat Insert

Insert rows into any table that exists in the schema. Required columns are automatically detected and filled with generated data.

```json
POST /api/populate
{
  "type": "tablename",
  "amount": 1000
}
```

Dependencies are resolved automatically (FK columns are populated from existing rows).

---

### 2. Organisation Unit Hierarchy

Creates a tree of organisation units across multiple levels.

```json
POST /api/populate
{
  "type": "organisationunit",
  "hierarchy": [5, 4, 3],
  "orgunitgroupid": 123
}
```

- `hierarchy`: number of nodes at each level — `[5, 4, 3]` creates 5 roots, 20 children (5×4), 60 grandchildren (5×4×3) = 85 total
- `orgunitgroupid`: optional — if provided, all created org units are added to this group

---

### 3. Category Model

Creates a full category combo structure: combos, categories, options, and all join table entries.

```json
POST /api/populate
{
  "type": "categorymodel",
  "categoryCombos": 2,
  "categoriesPerCombo": 3,
  "categoryOptionsPerCategory": 4
}
```

- Creates `categoryCombos` combos, each linked to `categoriesPerCombo` categories, each with `categoryOptionsPerCategory` options
- Option combinations (options^categories) must not exceed 500 per combo
- Also populates: `categorycombos`, `dataelementcategory`, `categoryoption`, `categorycategoryoptions`, `categorycombocategories`, `categoryoptioncombos`, `categoryoptioncombooptions`

---

### 4. Category Dimension

Inserts rows into the `categorydimension` table, each assigned a randomly selected category from the provided list.

```json
POST /api/populate
{
  "type": "categorydimension",
  "categories": [101, 102, 103],
  "amount": 500
}
```

- `categories`: list of existing `categoryid` values to randomly assign

---

### 5. Data Element

Inserts data elements, each assigned a randomly selected category combo.

```json
POST /api/populate
{
  "type": "dataelement",
  "amount": 100,
  "categoryComboIds": [1, 2, 3],
  "valueType": "TEXT",
  "domainType": "AGGREGATE",
  "aggregationType": "DEFAULT"
}
```

- `categoryComboIds`: list of existing `categorycomboid` values (required)
- `valueType`: `TEXT` | `NUMBER` (default: `TEXT`)
- `domainType`: `AGGREGATE` | `TRACKER` (default: `AGGREGATE`)
- `aggregationType`: `SUM` | `AVERAGE` | `NONE` | `DEFAULT` (default: `DEFAULT`)

---

### 6. Data Set

Inserts data sets, each assigned a randomly selected category combo and period type.

```json
POST /api/populate
{
  "type": "dataset",
  "amount": 50,
  "categoryComboIds": [1, 2],
  "periodTypeIds": [1, 2, 3]
}
```

- `categoryComboIds`: list of existing `categorycomboid` values (required)
- `periodTypeIds`: list of existing `periodtypeid` values (required)

---

### 7. Data Set Element

Creates matching data elements, data sets, and `datasetelement` join rows in one operation.

```json
POST /api/populate
{
  "type": "datasetelement",
  "amount": 30,
  "categoryComboIds": [1, 2]
}
```

- Creates `amount` data elements + `amount` data sets + `amount` join rows = 3× total rows

---

### 8. Program

Inserts programs of the specified type, each assigned a randomly selected category combo.

```json
POST /api/populate
{
  "type": "program",
  "amount": 20,
  "categoryComboIds": [1, 2],
  "programType": "WITHOUT_REGISTRATION"
}
```

- `programType`: `WITH_REGISTRATION` | `WITHOUT_REGISTRATION` (default: `WITHOUT_REGISTRATION`)

---

### 9. Program Indicator

Creates 10 programs plus the requested number of program indicators linked to them.

```json
POST /api/populate
{
  "type": "programindicator",
  "amount": 100,
  "categoryComboIds": [1, 2]
}
```

---

### 10. Data Approval Workflow

Inserts data approval workflow rows, each assigned a randomly selected category combo.

```json
POST /api/populate
{
  "type": "dataapprovalworkflow",
  "amount": 10,
  "categoryComboIds": [1, 2]
}
```

---

### 11. User Role

Creates user roles with associated authorities. Can specify explicit pairs or generate them randomly.

```json
POST /api/populate
{
  "type": "userrole",
  "userRoles": [
    {"role": "Admin", "authority": "ALL"},
    {"role": "Viewer", "authority": "F_VIEW_EVENT_ANALYTICS"}
  ]
}
```

Or generate randomly:
```json
{
  "type": "userrole",
  "amount": 5
}
```

Each role creates one `userrole` row + one `userroleauthorities` row.

---

### 12. User Group

Inserts user groups.

```json
POST /api/populate
{
  "type": "usergroup",
  "amount": 10
}
```

---

### 13. User Info (Full User)

Creates complete users with roles, groups, and all membership join table entries in a single operation.

```json
POST /api/populate
{
  "type": "userinfo",
  "amount": 50,
  "amountRoles": 3,
  "amountUserGroups": 5
}
```

- Creates `amountRoles` user roles + their authorities, `amountUserGroups` user groups, `amount` users, and all `userrolemembers` + `usergroupmembers` join rows

---

### 14. Org Unit Group

Inserts organisation unit groups.

```json
POST /api/populate
{
  "type": "orgunitgroup",
  "amount": 10
}
```

---

### 15. Chain

Runs multiple populate requests sequentially in a single job. Each sub-request can be any supported type.

```json
POST /api/populate
{
  "type": "chain",
  "requests": [
    {"type": "userrole", "amount": 3},
    {"type": "usergroup", "amount": 5},
    {"type": "userinfo", "amount": 20, "amountRoles": 3, "amountUserGroups": 5}
  ]
}
```

Sub-requests execute in order, making this useful for building up dependent data sets.

---

## How to Add a New Request Type

See [`CLAUDE.md`](CLAUDE.md) for the full developer guide. The short version:

1. **`PopulateRequest.java`** — add new fields and `isXxx()` / `hasXxx()` helper methods
2. **Create `XxxInsertService.java`** — implement batch insert logic using `SchemaDetectionService` for column discovery and the max-PK pattern for ID generation
3. **`PopulatorService.java`** — add routing (`if (request.isXxx()) return startXxxJob(request)`) and a `startXxxJob()` method
4. **`AsyncJobExecutor.java`** — inject the new service and add an `@Async` method
5. **`PopulatorController.java`** — add `request.isXxx()` to the `isSpecializedType` check if the type doesn't require `amount` or a schema table lookup

Key rules for insert services:
- Never hardcode column lists — use `schemaService.getTable("tablename").getInsertableColumns()`
- Never use `RETURNING` or auto-generated keys — use `SELECT COALESCE(MAX(pk_col), 0)` then assign IDs explicitly
- Always commit per batch and report progress via the `ProgressCallback`
