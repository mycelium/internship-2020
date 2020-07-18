package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

import lombok.Getter;

import java.util.*;

public abstract class ConstraintsManager {

    /**
     * Key: (schema name, table name), Value: list of constraints
     */
    @Getter
    protected Map<List<String>, List<Constraint>> tableConstraints = new HashMap<>();

    abstract Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop);

    abstract public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName);

    /**
     * Method removes one constraint from table
     */
    public Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType){
        return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, true);
    }

    /**
     * Method restores one constraint
     */
    public Constraint restoreOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean passException){

        try{
            return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, false);
        }catch (RuntimeException e){
            if(!passException)
                throw e;
            System.out.println(e.getMessage());
        }

        return null;
    }


    /**
     * Restores all constraints for the given table.
     * When the method is executed, exceptions will often be printed,
     * but when used correctly, this is only due to a double attempt to restore the indices for PK and FK,
     * which does not violate the correct operation of the method.
     */
    public void restoreAllConstraintsInTable(String schemaName, String tableName, boolean passException){

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));
        for(var c: constraints){
            if(c.isDropped()){
                try{
                    switchOneConstraint(schemaName,tableName,c.getConstraintName(), c.getContype(), false );
                }catch (RuntimeException e){
                    if(!passException)
                        throw e;
                    System.out.println(e.getMessage());
                }
            }
        }
    }


    /**
     * Drops all constraints for the given table.
     * When the method is executed, exceptions will often be printed,
     * but when used correctly, this is only due to a double attempt to drop the indices for PK and FK,
     * which does not violate the correct operation of the method.
     */
    public List<Constraint>  dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes){
        for(var e : ConstraintTypes){
            if(!ConstraintType.isValidType(e))
                throw new RuntimeException("Invalid constraint type");
        }

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));
        Collections.sort(constraints, Comparator.comparingInt(Constraint::determinePriority));
        List<Constraint> dropped = new ArrayList<>();


        for(var c: constraints){
            if(!c.isDropped() && Arrays.stream(ConstraintTypes).anyMatch(type -> type.equals(c.getContype()))){
                try{

                    switchOneConstraint(schemaName,tableName, c.getConstraintName(), c.getContype(), true);

                }catch (RuntimeException e){
                    if(!passException)
                        throw e;

                    System.out.println(e.getMessage());
                }
                dropped.add(c);
            }
        }

        return dropped;
    }

    abstract public String driverType();

}
