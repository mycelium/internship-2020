package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class MariaDBConstraintsManager implements ConstraintsManager {

    @Autowired
    private JdbcTemplate jdbc;

    @Getter
    Map<List<String>, List<Constraint>> tableConstraints = new HashMap<>();

    @Override
    public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName)
    {

        List<Constraint> constraints = new ArrayList<>();

        //Получаю DDL код всей таблицы
        String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                (rs,i)->rs.getString(2));

        Scanner sc = new Scanner(tableDDL);
        Constraint constraint;

        //Построчно добавляю constraints, если они есть
        while(sc.hasNextLine()){

            String line = sc.nextLine();

            if(line.contains("NOT NULL")){

                String attName =  line.replaceAll(".*`([^\\s]*)`.*", "$1");
                constraints.add(Constraint.buildConstraintNotNull(schemaName, tableName,  attName));

            }
            if(line.contains("PRIMARY KEY")){
                //(`city`, `id`)
                String attrs =  line.replaceAll(".*(\\(.*\\)).*", "$1");
                constraint = Constraint.buildConstraintUCPF(0, "primary", ConstraintType.PK,
                        schemaName, tableName, attrs);
                constraint.setDropDDL("ALTER TABLE "+schemaName+"."+tableName+" DROP PRIMARY KEY;");
                constraint.setRestoreDDL("ALTER TABLE "+schemaName+"."+tableName+" ADD PRIMARY KEY "+attrs+" ;");
                constraints.add(constraint);
            }

            if(line.contains("FOREIGN KEY")){

                String conname = line.replaceAll(".*CONSTRAINT (`[^\\s]*`).*", "$1");
                conname = conname.substring(1,conname.length()-1);
                String ddl = line;
                if(ddl.charAt(ddl.length()-1) == ',')
                    ddl = ddl.substring(0, ddl.length()-1);
                constraint = Constraint.buildConstraintUCPF(0,conname, ConstraintType.FK, schemaName, tableName,
                        ddl);
                constraint.setDropDDL("ALTER TABLE "+schemaName+"."+tableName+" DROP FOREIGN KEY "+conname+";");
                constraint.setRestoreDDL("ALTER TABLE "+schemaName+"."+tableName+" ADD "+ddl+";");
                constraints.add(constraint);

            }

            if(line.contains("DEFAULT")){

                String attName =  line.replaceAll(".*`([^\\s]*)`.*", "$1");
                String defaultValue = line.replaceAll(".*DEFAULT ([^\\s]*).*", "$1");
                if(defaultValue.charAt(0) == '\'')
                    defaultValue = line.replaceAll(".*DEFAULT ('[^']*').*", "$1");
                if(defaultValue.charAt(defaultValue.length()-1) == ',')
                    defaultValue = defaultValue.substring(0, defaultValue.length()-1);

                constraint = Constraint.buildConstraintDefault(schemaName,tableName,attName,"", defaultValue);
                constraints.add(constraint);

            }

            //Получаю implicit checks
            if(line.contains("CHECK") && !line.contains("CONSTRAINT")){

                String attName =  line.replaceAll(".*`([^\\s]*)`.*", "$1");
                String ddl = line.replaceAll(".*CHECK \\((.*)\\)", "$1");
                constraint = Constraint.buildConstraintUCPF(-1, attName,
                        ConstraintType.CHECK, schemaName,
                        tableName, "CHECK (" + ddl + ")");
                constraints.add(constraint);

            }

        }

        //Получаю check constraints

        jdbc.query( "select * from information_schema.CHECK_CONSTRAINTS "+
                        "where constraint_schema = '"+schemaName+"' and table_name = '"+tableName+"';",
                (rs, i) -> {
                    Constraint c = Constraint.buildConstraintUCPF(0,
                            rs.getString("constraint_name"),
                            ConstraintType.CHECK, schemaName,
                            tableName, "CHECK (" + rs.getString("check_clause") + ")");
                    //Проверяю повторную вставку для implicit checks
                    for(var e: constraints){
                        if(e.getContype() == ConstraintType.CHECK && e.getConname().equals(c.getConname()))
                            return null;
                    }

                    return null;
                });


        tableConstraints.remove(Arrays.asList(schemaName, tableName));
        tableConstraints.put(Arrays.asList(schemaName, tableName), constraints);
        return constraints;
    }

    @Override
    public String getTableOwner(String schemaName, String tableName) {
        return null;
    }

    @Override
    public int getSchemaPrivileges(String schemaName) {
        return 0;
    }

    @Override
    public Constraint dropOneConstraint(String schemaName, String tableName, String constraint, String constraintType) {
        return switchOneConstraint(schemaName,  tableName,  constraint,  constraintType, true);
    }

    @Override
    public Constraint restoreOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean passException) {
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

                            //Исключительная обработка для implicit checks
                            if(c.getOid() == -1 && c.getContype().equals(ConstraintType.CHECK)){

                                String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                                        (rs,i)->rs.getString(2));
                                Scanner sc = new Scanner(tableDDL);

                                //Нахожу строку с определением атрибута
                                String def = "";
                                while(sc.hasNextLine()) {
                                    def = sc.nextLine();
                                    if(def.contains("`"+c.getConname()+"`"))
                                        break;
                                }

                            }


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

                        //Обработка constraint NOT_NULL в MariaDB отличается от Postgres (вместо DROP NOT NULL надо использовать MODIFY column)
                    case ConstraintType.NOT_NULL:
                        if(c.getContype().equals(ConstraintType.NOT_NULL) && c.getAttname().equals(constraint)){
                            //DDL код для создания таблицы
                            String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                                    (rs,i)->rs.getString(2));
                            Scanner sc = new Scanner(tableDDL);

                            //Нахожу строку с определением атрибута
                            String def = "";
                            while(sc.hasNextLine()) {
                                def = sc.nextLine();
                                if(def.contains("`"+c.getAttname()+"`"))
                                    break;
                            }

                            //Вставляю или удаляю NOT NULL
                            if(drop == false){
                                def = def.replaceAll("(`[^\\s]*`) ([^\\s]*) ((UNSIGNED)|(unsigned) )?(.*)", "$1 $2 $3 NOT NULL $6");
                            }else{
                                def = def.replaceAll("NOT NULL", "");
                            }
                            //Удаляю запятую на конце
                            if(def.charAt(def.length()-1) == ',')
                                def = def.substring(0, def.length()-1);

                            //Выполняю запрос
                            jdbc.execute("ALTER TABLE "+schemaName+"."+tableName+" MODIFY "+def+" ;");
                            c.setDropped(drop);
                            return c;
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




        @Override
    public void restoreAllConstraintsInTable(String schemaName, String tableName, boolean passException) {

    }

    @Override
    public List<Constraint> dropAllConstraintsInTable(String schemaName, String tableName, boolean passException, String... ConstraintTypes) {
        return null;
    }

    @Override
    public String driverType() {
        return "MariaDB";
    }
}
