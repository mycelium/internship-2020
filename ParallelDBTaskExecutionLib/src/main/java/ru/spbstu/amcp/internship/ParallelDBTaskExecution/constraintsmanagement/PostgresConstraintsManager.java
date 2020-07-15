package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;


import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;

@Component
public class PostgresConstraintsManager implements ConstraintsManager {

    @Autowired
    private JdbcTemplate jdbc;

    public PostgresConstraintsManager(){

    }

    /**
     * Ключ: (имя схемы, имя таблицы), Значение: список constraints
     */
    @Getter
    Map<List<String>, List<Constraint>> tableConstraints = new HashMap<>();



    /**
     * Метод запоминает и возвращает все имеющиеся constraints для заданной таблицы в схеме
     * @param schemaName
     * @param tableName
     * @return
     */
    public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName){


        //Получить constraints вида: UNIQUE, CHECK, PK, FK
        List<Constraint> constraints = jdbc.query("select con.oid, conname, contype, nspname, relname, (select * from pg_catalog.pg_get_constraintdef(con.oid)) as ddl " +
                "from pg_catalog.pg_constraint con join pg_catalog.pg_class rel " +
                "ON rel.oid = con.conrelid join pg_catalog.pg_namespace nsp " +
                "on nsp.oid = connamespace " +
                "where nspname = '"+schemaName+"' and relname = '"+tableName+"';",
                (rs, i) -> {
                        return Constraint.buildConstraintUCPF(rs.getInt("oid"),
                                              rs.getString("conname"),
                                              rs.getString("contype"),
                                              rs.getString("nspname"),
                                              rs.getString("relname"),
                                              rs.getString("ddl"));
                });

        //Получить constraints вида: NOT NULL, DEFAULT
        jdbc.query("SELECT nspname, relname, attname, typ.typname, attnotnull, atthasdef, pg_catalog.pg_get_expr(d.adbin, d.adrelid) " +
                "FROM pg_catalog.pg_attribute a " +
                "    join pg_catalog.pg_class rel on rel.oid = a.attrelid " +
                "    join pg_catalog.pg_namespace nsp on rel.relnamespace = nsp.oid " +
                "    left join pg_catalog.pg_attrdef d on (a.attrelid, a.attnum) = (d.adrelid,  d.adnum) " +
                "    join pg_catalog.pg_type typ on typ.oid = a.atttypid " +
                "    where not a.attisdropped " +
                "    and a.attnum   > 0 " +
                "    and nspname = '"+schemaName+"' and relname = '"+tableName+"';",
                (rs, i) -> {
                    String schema = rs.getString("nspname");
                    String table = rs.getString("relname");
                    String attname = rs.getString("attname");
                    String type = rs.getString("typname");
                    Boolean notnull = rs.getBoolean("attnotnull");
                    Boolean hasdefault = rs.getBoolean("atthasdef");
                    Object defaultValue = rs.getObject("pg_get_expr");

                    if(notnull){
                        constraints.add(Constraint.buildConstraintNotNull(schema, table, attname));
                    }

                    if(hasdefault) {
                        constraints.add(Constraint.buildConstraintDefault(schema, table, attname, type, (String) defaultValue));
                    }

                    return null;
                });


        //Получить Индексы
        jdbc.query("SELECT schemaname, tablename, indexname, indexdef " +
                "FROM pg_catalog.pg_indexes " +
                "where schemaname = '"+schemaName+"' and tablename = '"+tableName+"'",
                (rs, i)->{
                    constraints.add(Constraint.buildConstraintIndex(rs.getString("schemaname"),
                                                                    rs.getString("tablename"),
                                                                    rs.getString("indexname"),
                                                                    rs.getString("indexdef")));
                    return null;
                });

        tableConstraints.remove(Arrays.asList(schemaName, tableName));
        //Запоминаем constraints для таблицы из схемы
        tableConstraints.put(Arrays.asList(schemaName, tableName), constraints);
        return constraints;
    }

    /**
     * Метод удаляет все constraints заданных видов из таблицы.
     * При выполнении метода часто будут печататься exceptions, но при правильном
     * использовании это связано лишь с двойной попыткой дропнуть индексы для PK и FK, что никак
     * не нарушает правильность работы метода.
     */
    public List<Constraint>  dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes){

        for(var e : ConstraintTypes){
            if(!e.equals(ConstraintType.CHECK) && !e.equals(ConstraintType.UNIQUE)
                    && !e.equals(ConstraintType.DEFAULT) && !e.equals(ConstraintType.FK)
                    && !e.equals(ConstraintType.INDEX) && !e.equals(ConstraintType.PK) &&
                       !e.equals(ConstraintType.NOT_NULL))
                throw new RuntimeException("Invalid constraint type");
        }

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));
        Collections.sort(constraints, Comparator.comparingInt(Constraint::determinePriority));

        List<Constraint> dropped = new ArrayList<>();


        for(var c: constraints){
            if(!c.isDropped() && Arrays.stream(ConstraintTypes).anyMatch(type -> type.equals(c.getContype()))){
                try{
                    jdbc.execute(c.getDropDDL());
                }catch (RuntimeException e){
                    if(!passException)
                        throw e;

                    System.out.println(e.getMessage());
                }
                c.setDropped(true);
                dropped.add(c);
            }
        }

        return dropped;

    }

    @Override
    public String driverType() {
        return "Postgres";
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
                    jdbc.execute(c.getRestoreDDL());
                }catch (RuntimeException e){
                    if(!passException)
                        throw e;
                    System.out.println(e.getMessage());
                }
                c.setDropped(false);
            }
        }
    }

    /**
     * Метод восстнавливает один constraint
     */
    public Constraint restoreOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean passException){
        if(!passException)
            return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, false);
        else{
            try{
                return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, false);
            }catch (RuntimeException e){
                System.out.println(e.getMessage());
            }
        }
        return null;
    }

    /**
     * Метод удаляет один constraint из таблицы
     */
    public Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType){
        return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, true);
    }

    /**
     * Метод переключает constraint из выключенного во включенное состояние (drop = false) и наоборот (drop = true)
     */
    private Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop){

        if(!constraintType.equals(ConstraintType.CHECK) && !constraintType.equals(ConstraintType.UNIQUE)
         && !constraintType.equals(ConstraintType.DEFAULT) && !constraintType.equals(ConstraintType.FK)
        && !constraintType.equals(ConstraintType.INDEX) && !constraintType.equals(ConstraintType.PK) &&
        !constraintType.equals(ConstraintType.NOT_NULL))
            throw new RuntimeException("Invalid constraint type");

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));

        Constraint con = Constraint.buildDummyConstraint();
        boolean performSwitching = false;
        loop: for (Constraint c: constraints){
            if(c.isDropped()==!drop && c.getSchemaName().equals(schemaName) && c.getTableName().equals(tableName)){

                switch (constraintType){
                    case ConstraintType.CHECK:
                    case ConstraintType.UNIQUE:
                    case ConstraintType.FK:
                    case ConstraintType.PK:
                        if((c.getContype().equals(ConstraintType.CHECK)||c.getContype().equals(ConstraintType.UNIQUE)||
                                c.getContype().equals(ConstraintType.FK)||c.getContype().equals(ConstraintType.PK))
                                && c.getConname().equals(constraint)){
                            performSwitching = true;
                            con = c;
                            break loop;
                        }
                        break;
                    case ConstraintType.INDEX:
                        if(c.getContype().equals(ConstraintType.INDEX) && c.getIndexName().equals(constraint)){
                            performSwitching = true;
                            con = c;
                            break loop;
                        }
                        break;
                    case ConstraintType.NOT_NULL:
                        if(c.getContype().equals(ConstraintType.NOT_NULL) && c.getAttname().equals(constraint)){
                            performSwitching = true;
                            con = c;
                            break loop;
                        }
                        break;
                    case ConstraintType.DEFAULT:
                        if(c.getContype().equals(ConstraintType.DEFAULT) && c.getAttname().equals(constraint)){
                            performSwitching = true;
                            con = c;
                            break loop;
                        }
                        break;
                }

            }
        }

        if(performSwitching){
            if(drop){
                jdbc.execute(con.getDropDDL());
                con.setDropped(true);
            }else{
                jdbc.execute(con.getRestoreDDL());
                con.setDropped(false);
            }
        }
        return con;
    }


    /**
     * Метод вернет владельца таблицы. В postgres только владелец таблицы может делать ALTER TABLE
     * https://www.postgresql.org/docs/8.1/sql-altertable.html
     * @param schemaName
     * @param tableName
     * @return
     */
    public String getTableOwner(String schemaName, String tableName){

        try {
            String tableOwner = jdbc.queryForObject("select tableowner from pg_tables " +
                            "where tablename = '" + tableName + "' and schemaname = '" + schemaName + "';",
                    (rs, i) -> rs.getString("tableowner"));
            return tableOwner;
        }catch (RuntimeException e){
            System.out.println("No such table or schema");
            throw  e;
        }
    }

    /**
     * Метод возвращает привелегии пользователся - CREATE - пользователь может изменять схему,
     *                                             USAGE - пользователь может использовать схему,
     *                                             (при условии, что объекты, лежащие внутри схемы
     *                                             имеют соответствующие права)
     */
    public int getSchemaPrivileges(String schemaName){

        List<Boolean> privileges = jdbc.queryForObject("SELECT" +
                "    pg_catalog.has_schema_privilege(" +
                "                    current_user, '"+schemaName+"', 'CREATE') AS c," +
                "    pg_catalog.has_schema_privilege(" +
                "                    current_user, '"+schemaName+"', 'USAGE') AS u;", (rs, i) ->
                Arrays.asList(rs.getBoolean("c"), rs.getBoolean("u")) );

        if(privileges.get(0) == true && privileges.get(1) == true)
            return PostgresSchemaPrivileges.CREATE_USAGE;
        else if(privileges.get(0) == true)
            return PostgresSchemaPrivileges.CREATE;
        else if(privileges.get(1) == true)
            return PostgresSchemaPrivileges.USAGE;

        return PostgresSchemaPrivileges.NO_RIGHTS;
    }

}


