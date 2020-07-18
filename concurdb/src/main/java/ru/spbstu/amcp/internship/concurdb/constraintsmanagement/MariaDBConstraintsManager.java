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
     * Remembers and returns all constraints for the given table in the schema.
     */
    @Override
    public List<Constraint> getAndInitAllConstraints(String schemaName, String tableName)
    {

        List<Constraint> constraints = new ArrayList<>();

        //table DDL
        String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                (rs,i)->rs.getString(2));

        Scanner sc = new Scanner(tableDDL);
        Constraint constraint;

        while(sc.hasNextLine()){

            String line = sc.nextLine();
            if(line.charAt(0) == ')')
                break;

            if(line.contains("NOT NULL")){
                setNotNullConstraint(schemaName, tableName, constraints, line);
            }
            if(line.contains("PRIMARY KEY")){
                setPKConstraint(schemaName, tableName, constraints, line);
            }

            if(line.contains("FOREIGN KEY")){
                setFKConstraint(schemaName, tableName, constraints, line);
            }

            if(line.contains("DEFAULT")){
                setDefaultConstraint(schemaName, tableName, constraints, line);
            }

            if(line.contains("CHECK") && !line.contains("CONSTRAINT")){
                setImplicitCheckConstraint(schemaName, tableName, constraints, line);
            }

            if((line.contains("KEY") && !line.contains("PRIMARY") && !line.contains("FOREIGN"))){
                setKeyConstraint(schemaName, tableName, constraints, line);
            }

        }

        setExplicitChecks(schemaName, tableName, constraints);


        tableConstraints.remove(Arrays.asList(schemaName, tableName));
        tableConstraints.put(Arrays.asList(schemaName, tableName), constraints);
        return constraints;
    }

    private void setNotNullConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Pattern p = Pattern.compile("`([^`\\s]*)`");
        Matcher m = p.matcher(line);
        m.find();
        String attName =  m.group(1);
        constraints.add(Constraint.buildConstraintNotNull(schemaName, tableName,  attName));
    }

    private void setPKConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Constraint constraint;//(`city`, `id`)
        String attrs =  line.replaceAll(".*(\\(.*\\)).*", "$1");
        constraint = Constraint.buildConstraintUCPF(0, "primary", ConstraintType.PK,
                schemaName, tableName, attrs);
        constraint.setDropDDL("ALTER TABLE "+schemaName+"."+tableName+" DROP PRIMARY KEY;");
        constraint.setRestoreDDL("ALTER TABLE "+schemaName+"."+tableName+" ADD PRIMARY KEY "+attrs+" ;");
        constraints.add(constraint);
    }

    private void setExplicitChecks(String schemaName, String tableName, List<Constraint> constraints) {
        jdbc.query( "select * from information_schema.CHECK_CONSTRAINTS "+
                        "where constraint_schema = '"+schemaName+"' and table_name = '"+tableName+"';",
                (rs, i) -> {
                    Constraint c = Constraint.buildConstraintUCPF(0,
                            rs.getString("constraint_name"),
                            ConstraintType.CHECK, schemaName,
                            tableName, "CHECK (" + rs.getString("check_clause") + ")");

                    for(var e: constraints){
                        if(e.getContype() == ConstraintType.CHECK && e.getConname().equals(c.getConname()))
                            return null;
                    }
                    constraints.add(c);
                    return null;
                });
    }

    private void setFKConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Constraint constraint;
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

    private void setDefaultConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Constraint constraint;
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

    private void setKeyConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Constraint constraint;
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

    private void setImplicitCheckConstraint(String schemaName, String tableName, List<Constraint> constraints, String line) {
        Constraint constraint;
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

    /**
     * Privileges
     * Executing the ALTER TABLE statement generally requires at least the ALTER privilege for the table or the database..
     *
     * If you are renaming a table, then it also requires the DROP, CREATE and INSERT privileges for the table or the database as well.
     *
     * Rreturns a list of rights for the current user to the table.
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
     * Returns a list of rights for the current user to the schema.
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
     * Returns a list of global rights for the current user.
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
     * Switches constraint from off to on (drop = false) and vice versa (drop = true)
     */
    Constraint switchOneConstraint(String schemaName, String tableName, String constraint, String constraintType, boolean drop){

        if(!ConstraintType.isValidType(constraintType))
            throw new RuntimeException("Invalid constraint type");

        List<Constraint> constraints = tableConstraints.get(Arrays.asList(schemaName, tableName));

        for (Constraint c: constraints){
            if(c.isDropped()==!drop && c.getSchemaName().equals(schemaName) && c.getTableName().equals(tableName)
              && c.getConstraintName().equals(constraint) && c.getContype().equals(constraintType)){


                if(c.getContype().equals(ConstraintType.PK) && REMOVE_AUTO_INCREMENT_BEFORE_PK && drop){
                    removeAutoIncrement(schemaName, tableName);
                }

                if(c.getOid() == -1 && c.getContype().equals(ConstraintType.CHECK)){
                    return dropImplicitCheck(schemaName, tableName, drop, c);
                }

                if(c.getContype().equals(ConstraintType.NOT_NULL) && c.getAttname().equals(constraint)){
                    return dropNotNull(schemaName, tableName, drop, c);
                }

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

    private Constraint dropImplicitCheck(String schemaName, String tableName, boolean drop, Constraint c) {
        String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                (rs,i)->rs.getString(2));
        Scanner sc = new Scanner(tableDDL);

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

        if(drop == false){
            def = def.concat(" " + c.getDefDDL());
        }else{
            def = def.replaceAll("CHECK \\((.*)\\).*", "");
        }

        jdbc.execute("ALTER TABLE "+schemaName+"."+tableName+" MODIFY COLUMN "+def+" ;");
        c.setDropped(drop);
        return c;
    }

    private Constraint dropNotNull(String schemaName, String tableName, boolean drop, Constraint c) {
        String tableDDL = jdbc.queryForObject("SHOW CREATE TABLE "+schemaName+"."+tableName+";",
                (rs,i)->rs.getString(2));
        Scanner sc = new Scanner(tableDDL);

        String def = "";
        while(sc.hasNextLine()) {
            def = sc.nextLine();
            if(def.contains("`"+c.getAttname()+"`"))
                break;
        }

        if(drop == false){
            def = def.replaceAll("(`[^\\s]*`) ([^\\s]*) ((UNSIGNED)|(unsigned) )?(.*)", "$1 $2 $3 NOT NULL $6");
        }else{
            def = def.replaceAll("NOT NULL", "");
        }
        if(def.charAt(def.length()-1) == ',')
            def = def.substring(0, def.length()-1);

        jdbc.execute("ALTER TABLE "+schemaName+"."+tableName+" MODIFY "+def+" ;");
        c.setDropped(drop);
        return c;
    }

    private void removeAutoIncrement(String schemaName, String tableName) {
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


    @Override
    public String driverType() {
        return "MariaDB";
    }
}
