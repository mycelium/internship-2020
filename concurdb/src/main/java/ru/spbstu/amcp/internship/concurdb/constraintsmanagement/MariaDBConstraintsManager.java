package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class MariaDBConstraintsManager extends ConstraintsManager {

    @Autowired
    protected JdbcTemplate jdbc;

    public static boolean REMOVE_AUTO_INCREMENT_BEFORE_PK = false;

    public MariaDBConstraintsManager(JdbcTemplate jdbc){
        this.jdbc = jdbc;
    }

    public MariaDBConstraintsManager(){
    }

    /**
     * Метод запоминает и возвращает все имеющиеся constraints для заданной таблицы в схеме
     * @param schemaName
     * @param tableName
     * @return
     */
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
            if(line.charAt(0) == ')')
                break;

            if(line.contains("NOT NULL")){

                Pattern p = Pattern.compile("`([^`\\s]*)`");
                Matcher m = p.matcher(line);
                m.find();
                String attName =  m.group(1);
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

                Pattern p = Pattern.compile("`([^`\\s]*)`");
                Matcher m = p.matcher(line);
                m.find();
                String attName =  m.group(1);

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

                Pattern p = Pattern.compile("`([^`\\s]*)`");
                Matcher m = p.matcher(line);
                m.find();
                String attName =  m.group(1);

                String ddl = line.replaceAll(".*CHECK \\((.*)\\).*", "$1");
                constraint = Constraint.buildConstraintUCPF(-1, attName,
                        ConstraintType.CHECK, schemaName,
                        tableName, "CHECK (" + ddl + ")");
                constraints.add(constraint);

            }

            //Получаю Unique
            if((line.contains("KEY") && !line.contains("PRIMARY") && !line.contains("FOREIGN"))){

                Pattern p = Pattern.compile("`([^`\\s]*)`");
                Matcher m = p.matcher(line);
                m.find();
                String indexName =  m.group(1);
                String ddl = line;
                if(ddl.charAt(ddl.length()-1) == ',')
                    ddl = ddl.substring(0, ddl.length()-1);
                if(line.contains("UNIQUE KEY"))
                    constraint = Constraint.buildConstraintUCPF(0, indexName, ConstraintType.UNIQUE,schemaName,tableName, ddl);
                else
                    constraint = Constraint.buildConstraintIndex(schemaName,tableName,indexName,ddl);
                constraint.setDropDDL("ALTER TABLE "+schemaName+"."+tableName+" DROP KEY " + indexName + " ;");
                constraint.setRestoreDDL("ALTER TABLE "+schemaName+"."+tableName+" ADD " + ddl + " ;");
                constraints.add(constraint);

            }

        }

        //Получаю explicit check constraints
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
                    constraints.add(c);
                    return null;
                });


        tableConstraints.remove(Arrays.asList(schemaName, tableName));
        tableConstraints.put(Arrays.asList(schemaName, tableName), constraints);
        return constraints;
    }

    /**
     * Privileges
     * Executing the ALTER TABLE statement generally requires at least the ALTER privilege for the table or the database..
     *
     * If you are renaming a table, then it also requires the DROP, CREATE and INSERT privileges for the table or the database as well.
     *
     * Метод вернет список прав для текущего пользователя на таблицу.
     */

    public List<String> getTablePrivilegesForCurrentUser(String schemaName, String tableName) {
        List<String> privs = new ArrayList<>();
        AtomicReference<Boolean> done = new AtomicReference<>(false);
        jdbc.query( "SHOW GRANTS FOR current_user",
                (rs, i) -> {
                    if(done.get())
                        return null;
                    String grant = rs.getString(1);
                    if(grant.contains("ON `"+schemaName+"`.`"+tableName+"` ")){
                        grant = grant.replaceFirst("GRANT (.*) ON .*","$1");
                        for (var p: grant.split(",")) {
                            privs.add(p);
                            done.set(true);
                        }
                    }
                    return null;
                });
        return privs;
    }

    /**
     * Privileges
     * Executing the ALTER TABLE statement generally requires at least the ALTER privilege for the table or the database..
     *
     * If you are renaming a table, then it also requires the DROP, CREATE and INSERT privileges for the table or the database as well.
     *
     * Метод вернет список прав для текущего пользователя на схему.
     *
     */
    public List<String> getSchemaPrivilegesForCurrentUser(String schemaName) {
        List<String> privs = new ArrayList<>();
        AtomicReference<Boolean> done = new AtomicReference<>(false);
        jdbc.query( "SHOW GRANTS FOR current_user",
                (rs, i) -> {
                    if(done.get())
                        return null;
                    String grant = rs.getString(1);
                    if(grant.contains("ON `"+schemaName+"`.* ") || grant.contains("ON `"+schemaName.replaceAll("_", "\\\\_")+"`.* ")) {
                        grant = grant.replaceFirst("GRANT (.*) ON .*","$1");
                        for (var p : grant.split(",")) {
                            privs.add(p);
                            done.set(true);
                        }
                    }
                    return null;
                });
        return privs;
    }


    /**
     * Privileges
     * Executing the ALTER TABLE statement generally requires at least the ALTER privilege for the table or the database..
     *
     * If you are renaming a table, then it also requires the DROP, CREATE and INSERT privileges for the table or the database as well.
     *
     * Метод вернет список глобальных прав для текущего пользователя
     *
     */
    public List<String> getGlobalPrivilegesForCurrentUser(){
        List<String> privs = new ArrayList<>();
        AtomicReference<Boolean> done = new AtomicReference<>(false);
        jdbc.query( "SHOW GRANTS FOR current_user",
                (rs, i) -> {
                    if(done.get())
                        return null;
                    String grant = rs.getString(1);
                    if(grant.contains("ON *.* ")){
                        grant = grant.replaceFirst("GRANT (.*) ON .*","$1");
                        for (var p: grant.split(",")) {
                            privs.add(p);
                            done.set(true);
                        }
                    }
                    return null;
                });
        return privs;
    }



    /**
     * Метод переключает constraint из выключенного во включенное состояние (drop = false) и наоборот (drop = true)
     */
    Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop){

        if(!ConstraintType.isValidType(constraintType))
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

                            //Удаление AUTO_INCREMENT (т.к. в MariaDB AUTO_INCREMENT без PK быть не может) (только если удаляется PK)
                            if(c.getContype().equals(ConstraintType.PK) && REMOVE_AUTO_INCREMENT_BEFORE_PK && drop){

                                String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                                        (rs,i)->rs.getString(2));
                                Scanner sc = new Scanner(tableDDL);

                                String def = "";
                                Boolean hasAutoincrement = false;
                                while(sc.hasNextLine()) {
                                    def = sc.nextLine();
                                    if(def.contains("AUTO_INCREMENT")) {
                                        hasAutoincrement = true;
                                        break;
                                    }
                                }
                                if(hasAutoincrement) {
                                    if (def.charAt(def.length() - 1) == ',')
                                        def = def.substring(0, def.length() - 1);
                                    def = def.replace("AUTO_INCREMENT", "");

                                    jdbc.execute("ALTER TABLE " + schemaName + "." + tableName + " MODIFY COLUMN " + def + " ;");
                                }
                            }

                            //Исключительная обработка для implicit checks
                            if(c.getOid() == -1 && c.getContype().equals(ConstraintType.CHECK)){

                                String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                                        (rs,i)->rs.getString(2));
                                Scanner sc = new Scanner(tableDDL);

                                //Нахожу строку с определением атрибута
                                String def = "";
                                while(sc.hasNextLine()) {
                                    def = sc.nextLine();
                                    Pattern p = Pattern.compile("`([^`\\s]*)`");
                                    Matcher m = p.matcher(def);
                                    m.find();
                                    String attName =  m.group(1);
                                    if(attName.equals(c.getConname()))
                                        break;
                                }

                                if(def.charAt(def.length()-1) == ',')
                                    def = def.substring(0, def.length()-1);

                                //Вставляю или удаляю implicit check
                                if(drop == false){
                                    def = def.concat(" " + c.getDefDDL());
                                }else{
                                    def = def.replaceAll("CHECK \\((.*)\\).*", "");
                                }

                                //Выполняю запрос
                                jdbc.execute("ALTER TABLE "+schemaName+"."+tableName+" MODIFY COLUMN "+def+" ;");
                                c.setDropped(drop);
                                return c;
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
    public String driverType() {
        return "MariaDB";
    }
}
