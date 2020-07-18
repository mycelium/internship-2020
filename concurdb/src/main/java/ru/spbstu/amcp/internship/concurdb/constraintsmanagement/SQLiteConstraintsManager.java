package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SQLiteConstraintsManager extends ConstraintsManager {

    @Autowired
    protected JdbcTemplate jdbc;

    public SQLiteConstraintsManager(JdbcTemplate jdbc)
    {
        this.jdbc = jdbc;
    }

    public SQLiteConstraintsManager(){
    }

    /**
     * Removes / restores index
     */
    @Override
    Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop) {

        if(constraintType != ConstraintType.INDEX)
            throw new RuntimeException("SQLite supports only indexes (drop/restore)");


        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));

        for (var c: constraints){

            if (c.isDropped() == !drop && c.getSchemaName().equals(schemaName) && c.getTableName().equals(tableName)
                    && c.getIndexName().equals(constraint)) {

                if(drop){
                    jdbc.execute(c.getDropDDL());
                    c.setDropped(true);
                }else{
                    jdbc.execute(c.getRestoreDDL());
                    c.setDropped(false);
                }
                return c;

            }

        }

        return null;
    }

    /**
     *  Remembers (plain, unique, partial) indexes for a given table in the schema
     */
    @Override
    public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName) {

        List<Constraint> indexConstraints = new ArrayList<>();

        List<String> indexNames = jdbc.query("PRAGMA "+schemaName+".index_list('"+tableName+"');",
                (rs, i) -> {
                    return rs.getString("name");
                });


        indexNames.stream().forEach(indexName -> {

            String ddl = jdbc.queryForObject("SELECT sql FROM "+schemaName+".sqlite_master\n" +
                    " where type = 'index' and tbl_name = '"+tableName+"'\n" +
                    "  and name = '"+indexName+"'", String.class);

            Constraint index = Constraint.buildConstraintIndex(schemaName, tableName, indexName, "");

            //Обязательно надо указать схему, так как полученный ddl её не содержит
            ddl = ddl.replaceFirst("INDEX " +indexName+ "( .*)", "INDEX " +schemaName+"."+indexName+ "$1");

            index.setRestoreDDL(ddl);
            index.setDropDDL("DROP INDEX IF EXISTS "+schemaName+"."+indexName+" ;");

            indexConstraints.add(index);

        });

        tableConstraints.remove(Arrays.asList(schemaName, tableName));
        tableConstraints.put(Arrays.asList(schemaName, tableName), indexConstraints);
        return indexConstraints;
    }


    @Override
    public String driverType() {
        return "SQLite";
    }

    /**
     * Removes all indexes for the table.
     */
    @Override
    public List<Constraint>  dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes){

        if(ConstraintTypes.length > 1 || !Arrays.stream(ConstraintTypes)
                .allMatch(type -> type.equals(ConstraintType.INDEX))){
            throw new RuntimeException("Only Indexes are supported in SQLite to drop/restore");
        }

        List<Constraint> indexConstraints = tableConstraints.get(Arrays.asList(schemaName, tableName));
        List<Constraint> dropped = new ArrayList<>();

        Arrays.stream(ConstraintTypes).forEach(type -> {

            for(var c: indexConstraints){
                if(!c.isDropped()){
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


        });

        return dropped;
    }


    /**
     * Restores all constraints for the given table.
     */
    @Override
    public void restoreAllConstraintsInTable(String schemaName, String tableName, boolean passException){

        for(var c: tableConstraints.get(Arrays.asList(schemaName, tableName))){
            if(c.isDropped()){
                try{
                    switchOneConstraint(schemaName,tableName, c.getConstraintName(), c.getContype(), false);
                }catch (RuntimeException e){
                    if(!passException)
                        throw e;
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
