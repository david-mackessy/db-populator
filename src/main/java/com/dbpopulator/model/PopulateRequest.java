package com.dbpopulator.model;

import java.util.List;

public record PopulateRequest(
    String type,
    int amount,
    List<Integer> hierarchy,
    Long orgunitgroupid,
    Integer categoryCombos,
    Integer categoriesPerCombo,
    Integer categoryOptionsPerCategory
) {
    public boolean hasHierarchy() {
        return hierarchy != null && !hierarchy.isEmpty();
    }

    public boolean isCategoryModel() {
        return "categorymodel".equalsIgnoreCase(type);
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
