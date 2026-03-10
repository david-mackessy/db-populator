package com.dbpopulator.service;

import com.dbpopulator.model.ColumnMetadata;
import com.dbpopulator.model.JsonValue;
import com.dbpopulator.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class DataGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(DataGeneratorService.class);

    private final DataSource dataSource;
    private final PrimaryKeyGeneratorService pkGenerator;
    private final Map<String, List<Object>> foreignKeyCache = new ConcurrentHashMap<>();

    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public DataGeneratorService(DataSource dataSource, PrimaryKeyGeneratorService pkGenerator) {
        this.dataSource = dataSource;
        this.pkGenerator = pkGenerator;
    }

    public Map<String, Object> generateRow(TableMetadata table) {
        Map<String, Object> row = new LinkedHashMap<>();

        for (ColumnMetadata column : table.getInsertableColumns()) {
            try {
                Object value = generateValue(column, table.tableName());
                row.put(column.name(), value);
            } catch (Exception e) {
                log.error("Failed to generate value for column {}.{} (type: {})",
                    table.tableName(), column.name(), column.dataType(), e);
                throw e;
            }
        }

        // shortname must match name — find the name key case-insensitively and copy it
        String nameKey = null;
        String shortNameKey = null;
        for (String key : row.keySet()) {
            if ("name".equalsIgnoreCase(key)) nameKey = key;
            if ("shortname".equalsIgnoreCase(key)) shortNameKey = key;
        }
        if (shortNameKey != null && nameKey != null) {
            row.put(shortNameKey, row.get(nameKey));
        }

        // General rule: boolean columns always default to false
        for (ColumnMetadata column : table.getInsertableColumns()) {
            if (row.get(column.name()) == null) {
                int sqlType = column.sqlType();
                String dataType = column.dataType().toLowerCase();
                if (sqlType == java.sql.Types.BOOLEAN || sqlType == java.sql.Types.BIT || dataType.contains("bool")) {
                    row.put(column.name(), false);
                }
            }
        }

        return row;
    }

    public Object generateValue(ColumnMetadata column, String tableName) {
        // Primary key columns get unique sequential IDs
        if (column.isPrimaryKey() && !column.isAutoIncrement()) {
            return pkGenerator.getNextId(tableName, column.name());
        }

        // Special handling for DHIS2 enum columns - must be checked before FK/type-based generation
        if ("datadimensiontype".equalsIgnoreCase(column.name())) {
            return "DISAGGREGATION";
        }
        if ("programtype".equalsIgnoreCase(column.name()) || "type".equalsIgnoreCase(column.name())) {
            return "WITHOUT_REGISTRATION";
        }

        if (column.isForeignKey()) {
            return generateForeignKeyValue(column);
        }

        // Special handling for 'uid' columns
        if ("uid".equalsIgnoreCase(column.name())) {
            return generateUid();
        }

        // Special handling for 'created'/'lastupdated' columns - use current timestamp
        if ("created".equalsIgnoreCase(column.name()) || "lastupdated".equalsIgnoreCase(column.name())) {
            return new Timestamp(System.currentTimeMillis());
        }

        // Boolean columns with app-level non-null constraints default to false
        if ("enablechangelog".equalsIgnoreCase(column.name())) {
            return false;
        }

        return generateValueByType(column, tableName);
    }

    private String generateUid() {
        // 11 alphanumeric characters:
        // - Starts with a capital letter
        // - Contains at least 1 lowercase letter and 1 number
        String upperCase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerCase = "abcdefghijklmnopqrstuvwxyz";
        String digits = "0123456789";
        String allChars = upperCase + lowerCase + digits;

        StringBuilder sb = new StringBuilder(11);

        // First character: capital letter
        sb.append(upperCase.charAt(ThreadLocalRandom.current().nextInt(upperCase.length())));

        // Ensure at least 1 lowercase and 1 digit in remaining 10 chars
        // Pick random positions for the required lowercase and digit
        int lowerPos = ThreadLocalRandom.current().nextInt(1, 11);
        int digitPos;
        do {
            digitPos = ThreadLocalRandom.current().nextInt(1, 11);
        } while (digitPos == lowerPos);

        for (int i = 1; i < 11; i++) {
            if (i == lowerPos) {
                sb.append(lowerCase.charAt(ThreadLocalRandom.current().nextInt(lowerCase.length())));
            } else if (i == digitPos) {
                sb.append(digits.charAt(ThreadLocalRandom.current().nextInt(digits.length())));
            } else {
                sb.append(allChars.charAt(ThreadLocalRandom.current().nextInt(allChars.length())));
            }
        }

        return sb.toString();
    }

    private Object generateValueByType(ColumnMetadata column, String tableName) {
        String dataType = column.dataType().toLowerCase();
        int sqlType = column.sqlType();

        return switch (sqlType) {
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.NVARCHAR, Types.NCHAR ->
                generateString(column, tableName);
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT ->
                generateInteger(column);
            case Types.BIGINT ->
                generateLong();
            case Types.DECIMAL, Types.NUMERIC, Types.DOUBLE, Types.FLOAT, Types.REAL ->
                generateDouble();
            case Types.BOOLEAN, Types.BIT ->
                false;
            case Types.DATE ->
                generateDate();
            case Types.TIME ->
                generateTime();
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                generateTimestamp();
            default -> generateByTypeName(dataType, column, tableName);
        };
    }

    private Object generateByTypeName(String dataType, ColumnMetadata column, String tableName) {
        if (dataType.contains("uuid")) {
            return UUID.randomUUID();
        }
        if (dataType.contains("json") || dataType.contains("jsonb")) {
            if ("translations".equalsIgnoreCase(column.name())) {
                return new JsonValue("[]");
            }
            return new JsonValue("{}");
        }
        if (dataType.contains("text")) {
            return generateString(column, tableName);
        }
        if (dataType.contains("bytea") || dataType.contains("blob")) {
            return new byte[]{0x00, 0x01, 0x02};
        }
        if (dataType.contains("bool")) {
            return false;
        }
        if (dataType.contains("int")) {
            return generateInteger(column);
        }
        if (dataType.contains("serial")) {
            return null;
        }
        if (dataType.contains("geometry") || dataType.contains("geography")) {
            return null;
        }

        log.debug("Unknown type '{}' for column '{}', generating string", dataType, column.name());
        return generateString(column, tableName);
    }

    private String generateString(ColumnMetadata column, String tableName) {
        int maxLength = column.columnSize() != null ? Math.min(column.columnSize(), 255) : 50;

        // Generate a short UUID suffix (8 chars) to ensure uniqueness
        String uniqueSuffix = "-" + UUID.randomUUID().toString().substring(0, 8);

        // Calculate available length for the prefix (leave room for suffix)
        int availableLength = maxLength - uniqueSuffix.length();
        if (availableLength < 1) {
            // If column is too small for suffix, just use UUID
            return UUID.randomUUID().toString().substring(0, Math.min(maxLength, 36));
        }

        int prefixLength = ThreadLocalRandom.current().nextInt(1, Math.min(availableLength, 20) + 1);

        StringBuilder sb = new StringBuilder(prefixLength);
        for (int i = 0; i < prefixLength; i++) {
            sb.append(CHARS.charAt(ThreadLocalRandom.current().nextInt(CHARS.length())));
        }
        sb.append(uniqueSuffix);

        return sb.toString();
    }

    private int generateInteger(ColumnMetadata column) {
        return ThreadLocalRandom.current().nextInt(1, 1_000_000);
    }

    private long generateLong() {
        return ThreadLocalRandom.current().nextLong(1, 1_000_000_000L);
    }

    private double generateDouble() {
        return ThreadLocalRandom.current().nextDouble(0, 10000);
    }

    private java.sql.Date generateDate() {
        long minDay = LocalDate.of(2000, 1, 1).toEpochDay();
        long maxDay = LocalDate.now().toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return java.sql.Date.valueOf(LocalDate.ofEpochDay(randomDay));
    }

    private java.sql.Time generateTime() {
        int hours = ThreadLocalRandom.current().nextInt(24);
        int minutes = ThreadLocalRandom.current().nextInt(60);
        int seconds = ThreadLocalRandom.current().nextInt(60);
        return java.sql.Time.valueOf(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    }

    private Timestamp generateTimestamp() {
        long minTime = LocalDateTime.of(2000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        long maxTime = Instant.now().toEpochMilli();
        long randomTime = ThreadLocalRandom.current().nextLong(minTime, maxTime);
        return new Timestamp(randomTime);
    }

    private Object generateForeignKeyValue(ColumnMetadata column) {
        String cacheKey = column.referencedTable() + "." + column.referencedColumn();
        List<Object> existingIds = foreignKeyCache.computeIfAbsent(cacheKey, k -> fetchExistingIds(column));

        if (existingIds.isEmpty()) {
            log.warn("No existing IDs found for FK reference: {}", cacheKey);
            return null;
        }

        return existingIds.get(ThreadLocalRandom.current().nextInt(existingIds.size()));
    }

    private List<Object> fetchExistingIds(ColumnMetadata column) {
        List<Object> ids = new ArrayList<>();
        String sql = String.format("SELECT %s FROM %s LIMIT 1000",
            column.referencedColumn(), column.referencedTable());

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getObject(1));
            }
        } catch (SQLException e) {
            log.error("Failed to fetch existing IDs for {}.{}", column.referencedTable(), column.referencedColumn(), e);
        }

        return ids;
    }

    public void updateForeignKeyCache(String tableName, String columnName, Object newId) {
        String cacheKey = tableName + "." + columnName;
        foreignKeyCache.computeIfAbsent(cacheKey, k -> new ArrayList<>()).add(newId);
    }

    public void clearForeignKeyCache() {
        foreignKeyCache.clear();
    }

    public String generateRandomSuffix() {
        StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            sb.append(CHARS.charAt(ThreadLocalRandom.current().nextInt(CHARS.length())));
        }
        return sb.toString();
    }
}
