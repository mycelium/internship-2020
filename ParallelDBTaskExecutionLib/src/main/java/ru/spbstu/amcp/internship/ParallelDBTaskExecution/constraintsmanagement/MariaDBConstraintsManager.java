package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MariaDBConstraintsManager implements ConstraintsManager {

    @Override
    public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName) {
        return null;
    }

    @Override
    public String getTableOwner(String schemaName, String tableName) {
        return null;
    }

    @Override
    public int getSchemaPrivileges(String schemaName) {
        return 0;
    }

    @Override
    public Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType) {
        return null;
    }

    @Override
    public Constraint restoreOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean passException) {
        return null;
    }

    @Override
    public void restoreAllConstraintsInTable(String schemaName, String tableName, boolean passException) {

    }

    @Override
    public List<Constraint> dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes) {
        return null;
    }

    @Override
    public String driverType() {
        return "MariaDB";
    }
}
