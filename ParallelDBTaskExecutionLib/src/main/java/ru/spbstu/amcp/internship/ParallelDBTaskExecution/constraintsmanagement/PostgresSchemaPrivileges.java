package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

public interface PostgresSchemaPrivileges {
    int NO_RIGHTS = 0;
    int CREATE = 1;
    int USAGE = 2;
    int CREATE_USAGE = 3;
}
