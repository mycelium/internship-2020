package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;


import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;

@Repository
public class PostgresConstraintsManager {

    private JdbcTemplate jdbc;

    public PostgresConstraintsManager(JdbcTemplate jdbc) throws IOException {
        this.jdbc = jdbc;
        loadPostgresDDLAutoGenerationFunction();
    }


    List<Constraint> constraintsInDB = new ArrayList<>();




    /**
     * Метод возвращает DDL скрипт для создания таблицы, отедльно constraints и index.
     * @return
     */
    public String getTableDDL(String schemaName, String tableName){
        return jdbc.queryForObject("SELECT * FROM public.PostgresDDLAutoGenerationFunction('"+schemaName+"', '"+tableName+"');",
                (rs,i)->rs.getString(1));
    }


    /**
     * Загружаю в бд функцию, которая будет генерировать DDL скрипт со всеми constraints для заданной таблицы
     */
    private void loadPostgresDDLAutoGenerationFunction() throws IOException {

        Resource resource = new ClassPathResource("PostgresDDLAutoGenerationFunction.sql");
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.execute(jdbc.getDataSource());

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


//    public void dropConstraint(String tableName, String constraintName){
//        jdbc.update("ALTER TABLE "+tableName+" DROP CONSTRAINT " +constraintName+";");
//    }
//
//    public void addPrimaryKey(String tableName, String constraintName, List<String> fields){
//        String pk = fields.stream().collect(Collectors.joining(", "));
//        jdbc.update("ALTER TABLE "+tableName+" ADD CONSTRAINT "
//                + constraintName +" PRIMARY KEY ("+pk+");");
//    }
//
//
//
//
//    public void addForeignKey(String tableName, String constraintName,
//                              List<String> FKFields, String PKTable, List<String> PKFields
//    ){
//        String fk = FKFields.stream().collect(Collectors.joining(", "));
//        String pk = PKFields.stream().collect(Collectors.joining(", "));
//        jdbc.update("ALTER TABLE "+tableName+" ADD CONSTRAINT "+constraintName+" " +
//                "FOREIGN KEY ("+fk+") REFERENCES "+PKTable+" ("+pk+")");
//    }

}


