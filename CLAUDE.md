# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DB Populator is a Java 25 backend service that populates a DHIS2 PostgreSQL database with generated entries. It provides an async API for bulk data insertion with dependency-aware table population.

## Build & Run Commands

```bash
# Build (once project is set up with Maven/Gradle)
./mvnw clean package    # Maven

# Run
java -jar target/db-populator.jar

# Run tests
./mvnw test             # All tests
./mvnw test -Dtest=ClassName#methodName  # Single test (Maven)
```

## Architecture

### Core Components

1. **Schema Detection** - Connects via JDBC to detect DHIS2 database schema, tables, and foreign key relationships on startup

2. **Dependency Resolution** - Analyzes table dependencies to determine correct insertion order (parent tables before child tables)

3. **Batch Insert Engine** - Performs bulk inserts using JDBC batch operations for performance, using HikariCP

4. **Async Job Tracker** - Maintains in-memory state of ongoing insert operations with status polling

5. **Build Tool** - Uses Maven for build and dependency management

### API Design

- `POST /populate` - Async endpoint accepting JSON with `type` (e.g., "organisation unit") and `amount` (insert count). Returns job ID and status polling endpoint.
- `GET /status/{jobId}` - Returns current progress of a populate job

### Key Considerations

- DHIS2 has complex table relationships; schema detection must handle circular dependencies
- Use connection pooling (HikariCP) for batch insert performance
- Job state can be in-memory for MVP but consider persistence for production
