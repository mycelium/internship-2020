package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

import lombok.Getter;

import java.util.*;

public abstract class ConstraintsManager {

    /**
     * Ключ: (имя схемы, имя таблицы), Значение: список constraints
     */
    @Getter
    protected Map<List<String>, List<Constraint>> tableConstraints = new HashMap<>();

    abstract Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop);

    abstract public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName);

    /**
     * Метод удаляет один constraint из таблицы
     */
    public Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType){
        return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, true);
    }

    /**
     * Метод восстнавливает один constraint
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
     * Метод восстанавливает все constraints для заданной таблицы.
     * При выполнении метода часто будут печататься exceptions, но при правильном
     * использовании это связано лишь с двойной попыткой восстановить индексы для PK и FK, что никак
     * не нарушает правильность работы метода.
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
     * Метод удаляет все constraints заданных видов из таблицы.
     * При выполнении метода часто будут печататься exceptions, но при правильном
     * использовании это связано лишь с двойной попыткой дропнуть индексы для PK и FK, что никак
     * не нарушает правильность работы метода.
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
