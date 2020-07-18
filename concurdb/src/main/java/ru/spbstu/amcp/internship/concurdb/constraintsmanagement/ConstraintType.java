package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

public interface ConstraintType {
    String UNIQUE = "u";
    String CHECK = "c";
    String PK = "p";
    String FK = "f";
    String DEFAULT = "d";
    String INDEX = "i";
    String NOT_NULL = "n";

    static boolean isValidType(String e){
        return "ucpfdin".contains(e);
    }

}
