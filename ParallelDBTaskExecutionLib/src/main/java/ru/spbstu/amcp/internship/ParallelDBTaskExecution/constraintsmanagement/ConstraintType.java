package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

public interface ConstraintType {
    String UNIQUE = "u";
    String CHECK = "c";
    String PK = "p";
    String FK = "f";
    String DEFAULT = "d";
    String INDEX = "i";
    String NOT_NULL = "n";
}
