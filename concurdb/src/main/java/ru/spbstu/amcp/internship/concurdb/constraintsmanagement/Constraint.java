package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;


import lombok.Getter;
import lombok.Setter;

/**
 * Possible constraints: unique, PK, FK, check, default, not null, index
 */
public class Constraint {

    /**
     * Only for u, c, p, f (otherwise 0)
     */
    @Getter
    private int oid;

    /**
     * Only for u,c,p,f (otherwise null)
     */
    @Getter
    private String conname;
    /**
     * Possible constraint type: u - unique, p - PK, f - FK, c - Check, d - default, n - not null, i - index
     */
    @Getter
    private String contype;

    /**
     * The name of the schema containing a constraint
     */
    @Getter
    private String schemaName;

    /**
     * The name of the table containing a constraint
     */
    @Getter
    private String tableName;
    /**
     * DDL code of constraint
     */
    @Getter
    private String defDDL = "";

    /**
     * The name of the constrained column (attribute).
     * Only for default and not null constraints
     */
    @Getter
    private String attname;

    /**
     * Only for default constraint (otherwise null).
     */
    @Getter
    private String defaultValueType;

    /**
     * Only for default constraint (otherwise null).
     */
    @Getter
    private String defaultValue;

    /**
     * Only for indexes (otherwise null).
     */
    @Getter
    private String indexName;


    @Getter
    @Setter
    private String restoreDDL;


    @Getter
    @Setter
    private String dropDDL;


    /**
     * Constraint status
     */
    @Getter @Setter
    boolean isDropped = false;

    /**
     * Creates new Unique or Check or PK or FK constraint depending on contype value
     */
    public static Constraint buildConstraintUCPF(int oid, String conname, String contype, String schemaName,
                                    String tableName, String ddl){
        Constraint ucpfc = new Constraint(oid, conname, contype, schemaName, tableName, ddl);

        //Default values for Postgres
        ucpfc.dropDDL = "ALTER TABLE "+ schemaName + "." + tableName + " DROP CONSTRAINT "+ conname +" ;";
        ucpfc.restoreDDL = "ALTER TABLE " + schemaName + "." + tableName + " ADD CONSTRAINT " +  conname +
                 " " + ddl + " ;";

        return ucpfc;
    }

    /**
     * Creates new not null constraint
     */
    public static Constraint buildConstraintNotNull(String schemaName,
                                                    String tableName, String attname){
        Constraint nnc =  new Constraint(schemaName, tableName, attname);

        //Default values for Postgres
        nnc.dropDDL = "ALTER TABLE "+schemaName+"."+tableName+" ALTER COLUMN "+attname+" DROP NOT NULL";
        nnc.restoreDDL = "ALTER TABLE "+schemaName+"."+tableName+" ALTER COLUMN "+attname+" SET NOT NULL";
        return nnc;


    }

    /**
     * Creates new default constraint
     */
    public static Constraint buildConstraintDefault(String schemaName,
                                                    String tableName, String attname,
                                                    String defValueType, String defValue){
        Constraint dc = new Constraint(schemaName,
                tableName, attname,
                defValueType, defValue);


        //Default values for Postgres
        dc.dropDDL = "ALTER TABLE "+schemaName+"."+tableName+" ALTER COLUMN "+attname+" DROP DEFAULT;";
        dc.restoreDDL = "ALTER TABLE "+schemaName+"."+tableName+" ALTER COLUMN "+attname+" SET DEFAULT " +
                ""+defValue+" ;";

        return dc;
    }

    /**
     * Creates new index constraint
     */
    public static Constraint buildConstraintIndex(String schemaName, String tableName, String indexName, String indexDef){
        Constraint ic = new Constraint(schemaName, tableName, indexName, indexDef);


        //Default values for Postgres
        ic.restoreDDL = ic.defDDL;
        ic.dropDDL = "DROP INDEX " + schemaName + "." + indexName + " ;";
        return ic;

    }


    private Constraint(String schemaName, String tableName, String indexName, String indexDef){

        contype = "i";
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        defDDL = indexDef;

    }

    private Constraint(int oid, String conname, String contype, String schemaName,
                          String tableName, String ddl){
        this.oid = oid;
        this.conname = conname;
        this.contype = contype;
        this.schemaName = schemaName;
        this.tableName = tableName;
        defDDL = ddl;
    }

    private Constraint(String schemaName,
                             String tableName, String attname){
        this.contype = "n";
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attname = attname;
    }

    private Constraint(String schemaName,
                       String tableName, String attname, String defValueType, String defValue){

        this.contype = "d";
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attname = attname;
        this.defaultValueType = defValueType;
        this.defaultValue = defValue;

    }

    private Constraint(){

    }

    public static Constraint buildDummyConstraint(){
        return new Constraint();
    }

    /**
     * Order must be observed (for example, not null cannot be removed from an attribute while there is a PK)
     * @return
     */
    int determinePriority(){
        switch (contype){
            case ConstraintType.PK:
                return 0;
            case ConstraintType.FK:
                return 1;
            case ConstraintType.UNIQUE:
                return 2;
            default:
                return 3;
        }
    }

    /**
     * Different names will be returned depending on the type of constraint
     */
    public String getConstraintName(){
        switch (contype){
            case ConstraintType.UNIQUE:
            case ConstraintType.CHECK:
            case ConstraintType.PK:
            case ConstraintType.FK:
                return conname;
            case ConstraintType.DEFAULT:
            case ConstraintType.NOT_NULL:
                return attname;
            case ConstraintType.INDEX:
                return indexName;
        }
        return null;
    }


}
