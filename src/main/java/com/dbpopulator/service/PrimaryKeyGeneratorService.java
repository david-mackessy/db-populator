package com.dbpopulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique primary key values by querying the current max ID from the database
 * and maintaining an AtomicLong counter for each table. This avoids the need to modify
 * the database schema to add auto-generation.
 */
@Service
public class PrimaryKeyGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(PrimaryKeyGeneratorService.class);

    private final DataSource dataSource;
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public PrimaryKeyGeneratorService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Gets the next unique primary key value for the given table and column.
     * On first call for a table, queries the database for MAX(column) and initializes
     * the counter from there.
     */
    public long getNextId(String tableName, String columnName) {
        String key = tableName + "." + columnName;
        AtomicLong counter = counters.computeIfAbsent(key, k -> initializeCounter(tableName, columnName));
        return counter.incrementAndGet();
    }

    /**
     * Initializes the counter by querying the current max value from the database.
     */
    private AtomicLong initializeCounter(String tableName, String columnName) {
        long maxValue = queryMaxValue(tableName, columnName);
        log.info("Initialized PK counter for {}.{} starting at {}", tableName, columnName, maxValue + 1);
        return new AtomicLong(maxValue);
    }

    private long queryMaxValue(String tableName, String columnName) {
        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", columnName, tableName);
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Failed to query max value for {}.{}, starting from 0", tableName, columnName, e);
        }
        return 0;
    }

    /**
     * Resets the counter for a specific table/column. Useful if the database state changes
     * outside of this service.
     */
    public void resetCounter(String tableName, String columnName) {
        String key = tableName + "." + columnName;
        counters.remove(key);
        log.debug("Reset counter for {}.{}", tableName, columnName);
    }

    /**
     * Clears all counters. Call this if the database is reset or truncated.
     */
    public void clearAllCounters() {
        counters.clear();
        log.info("Cleared all PK counters");
    }

    /**
     * Gets the current counter value without incrementing (for debugging/monitoring).
     */
    public Long getCurrentValue(String tableName, String columnName) {
        String key = tableName + "." + columnName;
        AtomicLong counter = counters.get(key);
        return counter != null ? counter.get() : null;
    }
}
