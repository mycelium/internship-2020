package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

public class Constraint {

    public static enum Type{
        NOT_NULL,
        UNIQUE,
        PRIMARY_KEY,
        FOREIGN_KEY,
        CHECK,
        DEFAULT,
        INDEX
    }

    /**
     * Тип constraint
     */
    Type typeOfConstraint;
    /**
     * SQL код для создания constraint
     */
    String ddl;
    /**
     * Название constraint
     */
    String constraintName;

    /**
     * Название таблицы, в которой присутствует constraint
     */
    String tableName;

    /**
     * Удален ли constraint
     */
    Boolean isDropped;

    public Constraint(String tableName, String constraintName, Type typeOfConstraint, String ddl){

        this.tableName = tableName;
        this.constraintName = constraintName;
        this.typeOfConstraint = typeOfConstraint;
        this.ddl = ddl;
        isDropped = false;

    }

}
