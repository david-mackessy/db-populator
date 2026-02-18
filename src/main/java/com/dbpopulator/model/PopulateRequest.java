package com.dbpopulator.model;

import java.util.List;
import java.util.Set;

public record PopulateRequest(
    String type,
    int amount,
    List<Integer> hierarchy,
    Long orgunitgroupid,
    Integer categoryCombos,
    Integer categoriesPerCombo,
    Integer categoryOptionsPerCategory,
    List<Long> categories,
    List<Long> categoryComboIds,
    String valueType,
    String domainType,
    String aggregationType,
    List<Long> periodTypeIds,
    String programType
) {
    public boolean hasHierarchy() {
        return hierarchy != null && !hierarchy.isEmpty();
    }

    public boolean isCategoryModel() {
        return "categorymodel".equalsIgnoreCase(type);
    }

    public boolean isCategoryDimension() {
        return "categorydimension".equalsIgnoreCase(type);
    }

    public boolean hasCategories() {
        return categories != null && !categories.isEmpty();
    }

    public boolean isDataElement() {
        return "dataelement".equalsIgnoreCase(type);
    }

    public boolean hasCategoryComboIds() {
        return categoryComboIds != null && !categoryComboIds.isEmpty();
    }

    public boolean isDataSet() {
        return "dataset".equalsIgnoreCase(type);
    }

    public boolean isDataSetElement() {
        return "datasetelement".equalsIgnoreCase(type);
    }

    public boolean hasPeriodTypeIds() {
        return periodTypeIds != null && !periodTypeIds.isEmpty();
    }

    public boolean isProgram() {
        return "program".equalsIgnoreCase(type);
    }

    private static final Set<String> VALID_PROGRAM_TYPES = Set.of("WITH_REGISTRATION", "WITHOUT_REGISTRATION");

    public String resolvedProgramType() {
        if (programType == null || programType.isBlank()) return "WITHOUT_REGISTRATION";
        String upper = programType.toUpperCase();
        if (!VALID_PROGRAM_TYPES.contains(upper)) {
            throw new IllegalArgumentException("Invalid programType '" + programType + "'. Allowed: " + VALID_PROGRAM_TYPES);
        }
        return upper;
    }

    private static final Set<String> VALID_VALUE_TYPES = Set.of("TEXT", "NUMBER");
    private static final Set<String> VALID_DOMAIN_TYPES = Set.of("AGGREGATE", "TRACKER");
    private static final Set<String> VALID_AGGREGATION_TYPES = Set.of("SUM", "AVERAGE", "NONE", "DEFAULT");

    public String resolvedValueType() {
        if (valueType == null || valueType.isBlank()) return "TEXT";
        String upper = valueType.toUpperCase();
        if (!VALID_VALUE_TYPES.contains(upper)) {
            throw new IllegalArgumentException("Invalid valueType '" + valueType + "'. Allowed: " + VALID_VALUE_TYPES);
        }
        return upper;
    }

    public String resolvedDomainType() {
        if (domainType == null || domainType.isBlank()) return "AGGREGATE";
        String upper = domainType.toUpperCase();
        if (!VALID_DOMAIN_TYPES.contains(upper)) {
            throw new IllegalArgumentException("Invalid domainType '" + domainType + "'. Allowed: " + VALID_DOMAIN_TYPES);
        }
        return upper;
    }

    public String resolvedAggregationType() {
        if (aggregationType == null || aggregationType.isBlank()) return "DEFAULT";
        String upper = aggregationType.toUpperCase();
        if (!VALID_AGGREGATION_TYPES.contains(upper)) {
            throw new IllegalArgumentException("Invalid aggregationType '" + aggregationType + "'. Allowed: " + VALID_AGGREGATION_TYPES);
        }
        return upper;
    }

    public int getTotalHierarchyCount() {
        if (!hasHierarchy()) {
            return 0;
        }
        int total = 0;
        int multiplier = 1;
        for (int count : hierarchy) {
            multiplier *= count;
            total += multiplier;
        }
        return total;
    }

    public int getTotalCategoryModelCount() {
        if (!isCategoryModel()) {
            return 0;
        }
        int combos = categoryCombos != null ? categoryCombos : 0;
        int catsPerCombo = categoriesPerCombo != null ? categoriesPerCombo : 0;
        int optsPerCat = categoryOptionsPerCategory != null ? categoryOptionsPerCategory : 0;

        int totalCategories = combos * catsPerCombo;
        int totalOptions = totalCategories * optsPerCat;
        return combos + totalCategories + totalOptions;
    }
}
