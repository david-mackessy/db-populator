# DB Populator Service
## What is it
The DB Populator service is a backend service written in Java. Its function is to populate a DHIS2 Postgres DB with entries.

## Requirements
- The service should be able to detect the DB schema with the given the JDBC connection details
- The service should be able to craft chosen inserts based on dependencies of that table
- The service should be performant and fast, using batch inserts where possible
- The service will be written in Java 25

## Features
1. Detect the DHIS2 DB on startup and understand its schema and dependencies/relationships
2. Create an API so that users can send JSON POST requests with their chosen populator query
    - The populator query will have fields:
        - type (e.g. organisation unit)
        - amount (number of desired inserts)
    - This will be an async request so the caller is not blocked
    - The API will return a JSON payload that includes a GET endpoint for updates on the requested insert
3. The service will need to keep an internal record of current inserts so it is able to respond to the GET current status query

