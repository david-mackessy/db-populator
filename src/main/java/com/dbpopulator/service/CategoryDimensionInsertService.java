package com.dbpopulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class CategoryDimensionInsertService {

    private static final Logger log = LoggerFactory.getLogger(CategoryDimensionInsertService.class);
    private static final int BATCH_SIZE = 1000;

    private final DataSource dataSource;

    public CategoryDimensionInsertService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int insertCategoryDimensions(int amount, List<Long> categoryIds, ProgressCallback callback) throws SQLException {
        log.info("Starting category dimension insert: {} rows with {} category IDs to choose from",
            amount, categoryIds.size());

        if (categoryIds.isEmpty()) {
            throw new IllegalArgumentException("categories array cannot be empty");
        }

        int inserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                // Get the current max PK value
                long nextPkValue = getMaxPkValue(conn) + 1;
                log.info("Starting categorydimensionid from {}", nextPkValue);

                String sql = "INSERT INTO categorydimension (categorydimensionid, categoryid) VALUES (?, ?)";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (int i = 0; i < amount; i++) {
                        long pkValue = nextPkValue + i;
                        long categoryId = categoryIds.get(ThreadLocalRandom.current().nextInt(categoryIds.size()));

                        ps.setLong(1, pkValue);
                        ps.setLong(2, categoryId);
                        ps.addBatch();

                        if ((i + 1) % BATCH_SIZE == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                            conn.commit();
                            inserted = i + 1;
                            if (callback != null) {
                                callback.onProgress(inserted);
                            }
                            log.debug("Committed batch, {} rows inserted so far", inserted);
                        }
                    }

                    // Execute remaining batch
                    if (amount % BATCH_SIZE != 0) {
                        ps.executeBatch();
                        conn.commit();
                        inserted = amount;
                        if (callback != null) {
                            callback.onProgress(inserted);
                        }
                    }
                }

                log.info("Category dimension insert complete: {} rows inserted", inserted);

            } catch (SQLException e) {
                conn.rollback();
                log.error("Category dimension insert failed, rolling back", e);
                throw e;
            }
        }

        return inserted;
    }

    private long getMaxPkValue(Connection conn) throws SQLException {
        String sql = "SELECT COALESCE(MAX(categorydimensionid), 0) FROM categorydimension";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(int totalInserted);
    }
}
