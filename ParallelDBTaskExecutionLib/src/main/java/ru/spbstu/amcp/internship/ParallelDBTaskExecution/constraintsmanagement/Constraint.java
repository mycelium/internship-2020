package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;


import lombok.Getter;
import lombok.Setter;

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
     * Код DDL для drop и restore
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
     *  DDL запрос на восстановление constraint
     */
    @Getter
    private String restoreDDL;

    /**
     * DDL запрос на удаление constraint
     */
    @Getter
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
        return new Constraint(schemaName,
                tableName, attname,
                defValueType, defValue);
    }

    /**
     * Создать index constraint
     */
    public static Constraint buildConstraintIndex(String schemaName, String tableName, String indexName, String indexDef){
        Constraint ic = new Constraint(schemaName, tableName, indexName, indexDef);

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


}
