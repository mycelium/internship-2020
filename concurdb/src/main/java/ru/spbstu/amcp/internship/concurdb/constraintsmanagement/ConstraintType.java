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
        if(!e.equals(ConstraintType.CHECK) && !e.equals(ConstraintType.UNIQUE)
                && !e.equals(ConstraintType.DEFAULT) && !e.equals(ConstraintType.FK)
                && !e.equals(ConstraintType.INDEX) && !e.equals(ConstraintType.PK) &&
                !e.equals(ConstraintType.NOT_NULL))
            return false;
        return true;
    }

}
