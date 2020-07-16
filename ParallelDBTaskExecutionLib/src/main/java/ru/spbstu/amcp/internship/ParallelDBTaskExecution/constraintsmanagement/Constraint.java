package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;


import lombok.Getter;
import lombok.Setter;

/**
 * Тип constraint - u - unique, p - PK, f - FK, c - Check, d - default, n - not null, i - индекс
 */
public class Constraint {

    /**
     * Только для u, c, p, f
     */
    @Getter
    private int oid;

    /**
     * Имя constraint - только для u,c,p,f
     */
    @Getter
    private String conname;
    /**
     * Тип constraint - u - unique, p - PK, f - FK, c - Check, d - default, n - not null, i - индекс
     */
    @Getter
    private String contype;
    /**
     * Имя схемы
     */
    @Getter
    private String schemaName;
    /**
     * Имя таблицы
     */
    @Getter
    private String tableName;
    /**
     * Код DDL
     */
    @Getter
    private String defDDL = "";

    /**
     * Имя атрибута - только для default и not null
     */
    @Getter
    private String attname;

    /**
     * Тип default value - только для default constraint
     */
    @Getter
    private String defaultValueType;

    /**
     * Default value - только для default constraint
     */
    @Getter
    private String defaultValue;

    /**
     * Имя индекса для constraint вида index
     */
    @Getter
    private String indexName;

    /**
     * DDL запрос на восстановление constraint
     */
    @Getter
    @Setter
    private String restoreDDL;

    /**
     * DDL запрос на удаление constraint
     */
    @Getter
    @Setter
    private String dropDDL;

    /**
     * Статус constraint
     */
    @Getter @Setter
    boolean isDropped = false;

    /**
     * Создать новый ucpf constraint
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
     * Создать not null constraint
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
     * Создать default constraint
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
     * Создать index constraint
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
     * При dropAllConstraints необходимо соблюдать порядок
     * (например, not null не может быть удален с атрибута, пока есть PK)
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
     * В зависимости от типа constraint будут возвращены разные наименования
     * @return
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
