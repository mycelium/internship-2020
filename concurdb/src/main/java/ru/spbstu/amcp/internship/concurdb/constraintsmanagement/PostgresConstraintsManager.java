package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class PostgresConstraintsManager extends ConstraintsManager {

    @Autowired
    private JdbcTemplate jdbc;

    public PostgresConstraintsManager(JdbcTemplate jdbc){
        this.jdbc = jdbc;
    }

    public PostgresConstraintsManager(){

    }

    /**
     * Remembers and returns all constraints for a given table in the schema
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


    @Override
    public String driverType() {
        return "Postgres";
    }


    /**
     * Switches constraint from off to on (drop = false) and vice versa (drop = true)
     */
    Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop){

        if(!ConstraintType.isValidType(constraintType))
            throw new RuntimeException("Invalid constraint type");

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));

        Constraint con = findConstraintToSwitch(schemaName, tableName, constraint, constraintType, drop, constraints);

        if(con != null){
            performSwitching(drop, con);
        }
        return con;
    }

    private Constraint findConstraintToSwitch(String schemaName, String tableName, String constraint, String constraintType, boolean drop, List<Constraint> constraints) {
        for (Constraint c: constraints){
            if(c.isDropped()==!drop && c.getSchemaName().equals(schemaName) && c.getTableName().equals(tableName)
                && c.getConstraintName().equals(constraint) && c.getContype().equals(constraintType)){
                    return c;
            }
        }
        return null;
    }

    private void performSwitching(boolean drop, Constraint con) {
        if(drop){
            jdbc.execute(con.getDropDDL());
            con.setDropped(true);
        }else{
            jdbc.execute(con.getRestoreDDL());
            con.setDropped(false);
        }
    }


    /**
     * Returns the owner of the table. In postgres, only the table owner can query ALTER TABLE
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
     * Returns user privileges - CREATE
     *                           USAGE
     *
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


