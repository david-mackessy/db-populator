package com.dbpopulator.service;

import com.dbpopulator.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DependencyResolver {

    private static final Logger log = LoggerFactory.getLogger(DependencyResolver.class);

    private final SchemaDetectionService schemaService;

    public DependencyResolver(SchemaDetectionService schemaService) {
        this.schemaService = schemaService;
    }

    public List<String> resolveInsertionOrder(String targetTable) {
        log.debug("Resolving insertion order for table: {}", targetTable);

        if (!schemaService.tableExists(targetTable)) {
            log.error("Cannot resolve dependencies - table not found: {}", targetTable);
            throw new IllegalArgumentException("Table not found: " + targetTable);
        }

        Set<String> visited = new HashSet<>();
        Set<String> inStack = new HashSet<>();
        List<String> result = new ArrayList<>();
        Set<String> cycleBreaks = new HashSet<>();

        collectDependencies(targetTable, visited, inStack, result, cycleBreaks);

        if (!cycleBreaks.isEmpty()) {
            log.warn("Circular dependencies detected for table {}. Breaking cycles at: {}", targetTable, cycleBreaks);
        }

        log.debug("Resolved insertion order for {}: {} tables", targetTable, result.size());
        return result;
    }

    private void collectDependencies(String tableName, Set<String> visited, Set<String> inStack,
                                     List<String> result, Set<String> cycleBreaks) {
        if (visited.contains(tableName)) {
            return;
        }

        if (inStack.contains(tableName)) {
            cycleBreaks.add(tableName);
            log.debug("Cycle detected at table: {}", tableName);
            return;
        }

        inStack.add(tableName);

        TableMetadata table = schemaService.getTable(tableName);
        if (table != null) {
            for (String dependency : table.dependsOn()) {
                if (schemaService.tableExists(dependency)) {
                    collectDependencies(dependency, visited, inStack, result, cycleBreaks);
                }
            }
        }

        inStack.remove(tableName);
        visited.add(tableName);
        result.add(tableName);
    }

    public List<String> topologicalSort() {
        Collection<TableMetadata> allTables = schemaService.getAllTables();
        Set<String> visited = new HashSet<>();
        Set<String> inStack = new HashSet<>();
        List<String> result = new ArrayList<>();
        Set<String> cycleBreaks = new HashSet<>();

        for (TableMetadata table : allTables) {
            if (!visited.contains(table.tableName())) {
                collectDependencies(table.tableName(), visited, inStack, result, cycleBreaks);
            }
        }

        if (!cycleBreaks.isEmpty()) {
            log.warn("Circular dependencies found in schema. Cycles broken at: {}", cycleBreaks);
        }

        return result;
    }

    public Set<String> findCircularDependencies() {
        Set<String> circularTables = new HashSet<>();
        Collection<TableMetadata> allTables = schemaService.getAllTables();

        for (TableMetadata table : allTables) {
            if (hasCircularDependency(table.tableName(), new HashSet<>())) {
                circularTables.add(table.tableName());
            }
        }

        return circularTables;
    }

    private boolean hasCircularDependency(String tableName, Set<String> path) {
        if (path.contains(tableName)) {
            return true;
        }

        TableMetadata table = schemaService.getTable(tableName);
        if (table == null) {
            return false;
        }

        path.add(tableName);
        for (String dep : table.dependsOn()) {
            if (hasCircularDependency(dep, path)) {
                return true;
            }
        }
        path.remove(tableName);

        return false;
    }
}
