package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

import java.util.List;

public interface ConstraintsManager {

    List<Constraint> getAndInitAllConstraints(String schemaName, String tableName);
    Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType);
    Constraint restoreOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean passException);
    void restoreAllConstraintsInTable(String schemaName, String tableName, boolean passException);
    List<Constraint>  dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes);
    String driverType();

}
