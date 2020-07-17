package ru.spbstu.amcp.internship.concurdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.ConstraintType;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.PostgresConstraintsManager;

import javax.sql.DataSource;
import java.util.List;

@SpringBootTest
public class PostgresTest {


    PostgresConstraintsManager pcm;

    JdbcTemplate jdbcTemplate;

    @Before
    public void init(){
        jdbcTemplate = new JdbcTemplate(dataSource());
        pcm = new PostgresConstraintsManager(jdbcTemplate);
        Resource resource = new ClassPathResource("postgrescar.sql");
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.execute(jdbcTemplate.getDataSource());
    }

    @Test
    public void testPostgresConstraintsManagerOnDropping_RestoringOneConstraints(){

        //ConstraintsManager запомнит все constraints для указанной таблицы в схеме
        //Это делать обязательно перед отключением constraints
        List<Constraint> cons = pcm.getAndInitAllConstraints("test_schema", "car");

        String ddlBeforeDropping = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NOT NULL DEFAULT 'BMW'::character varying,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL DEFAULT 5,\n" +
                "  autointf integer NOT NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6,\n" +
                "  CONSTRAINT car_pkey PRIMARY KEY (id),\n" +
                "  CONSTRAINT unique_name UNIQUE (name),\n" +
                "  CONSTRAINT distfk FOREIGN KEY (user_id, user_name) REFERENCES users(id, name) ON UPDATE CASCADE ON DELETE CASCADE,\n" +
                "  CONSTRAINT car_check CHECK (((value > 50) AND (autointf < 100)))\n" +
                ");\n" +
                "CREATE UNIQUE INDEX car_pkey ON test_schema.car USING btree (id);\n" +
                "CREATE UNIQUE INDEX unique_name ON test_schema.car USING btree (name);\n" +
                "CREATE INDEX car_upper_idx ON test_schema.car USING btree (upper((name)::text));\n" +
                "CREATE UNIQUE INDEX car_idx ON test_schema.car USING btree (name);\n";

        String ddlAfterDropping = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NULL,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL,\n" +
                "  autointf integer NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6\n" +
                ");\n";

        String ddlAfterExRestoring = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NOT NULL DEFAULT 'BMW'::character varying,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL DEFAULT 5,\n" +
                "  autointf integer NOT NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6,\n" +
                "  CONSTRAINT car_pkey PRIMARY KEY (id)\n" +
                ");\n" +
                "CREATE UNIQUE INDEX car_pkey ON test_schema.car USING btree (id);\n" +
                "CREATE INDEX car_upper_idx ON test_schema.car USING btree (upper((name)::text));\n";

        //Поменяны местами индексы - суть не меняется
        String ddlAfterExRestoring2 = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NOT NULL DEFAULT 'BMW'::character varying,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL DEFAULT 5,\n" +
                "  autointf integer NOT NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6,\n" +
                "  CONSTRAINT car_pkey PRIMARY KEY (id)\n" +
                ");\n" +
                "CREATE INDEX car_upper_idx ON test_schema.car USING btree (upper((name)::text));\n" +
                "CREATE UNIQUE INDEX car_pkey ON test_schema.car USING btree (id);\n";

        //Получаю DDL таблицы
        String ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);

        Assert.assertTrue(ddl.equals(ddlBeforeDropping));

        //Отключаем constraints
        pcm.dropOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE);
        pcm.dropOneConstraint("test_schema", "car", "car_pkey", ConstraintType.PK);
        pcm.dropOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT);
        pcm.dropOneConstraint("test_schema", "car", "autointf", ConstraintType.NOT_NULL);
        pcm.dropOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT);
        pcm.dropOneConstraint("test_schema", "car", "name", ConstraintType.NOT_NULL);
        pcm.dropOneConstraint("test_schema", "car", "distfk", ConstraintType.FK);
        pcm.dropOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK);
        pcm.dropOneConstraint("test_schema", "car", "car_idx", ConstraintType.INDEX);
        pcm.dropOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX);

        //Здесь вставятся данные, не удовлетворяющие constraints
        for (int i = 0; i < 100; i++){
            jdbcTemplate.execute("insert into test_schema.car (id, name, user_name, user_id," +
                    "value, autointf) VALUES ("+i+", 'Test', 'name', 1, 30, 100)");
        }


        ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);
        Assert.assertTrue(ddl.equals(ddlAfterDropping));

        //Включаем constraints - некоторые constraints не включатся, так как нарушены условия после вставки, но они (constraints) не удалятся,
        //пока работает программа
        pcm.restoreOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX,true);
        pcm.restoreOneConstraint("test_schema", "car", "car_idx", ConstraintType.INDEX,true);
        pcm.restoreOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE,true);
        pcm.restoreOneConstraint("test_schema", "car", "car_pkey", ConstraintType.PK,true);
        pcm.restoreOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT,true);
        pcm.restoreOneConstraint("test_schema", "car", "autointf", ConstraintType.NOT_NULL,true);
        pcm.restoreOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT,true);
        pcm.restoreOneConstraint("test_schema", "car", "name", ConstraintType.NOT_NULL,true);
        pcm.restoreOneConstraint("test_schema", "car", "distfk", ConstraintType.FK,true);
        pcm.restoreOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK,true);



        ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);

        Assert.assertTrue(ddl.contains(ddlAfterExRestoring) || ddl.contains(ddlAfterExRestoring2));

    }


    @Test
    public void testPostgresConstraintsManagerOnDroppingAllConstraints() {

        //24 переста
        String ddlBeforeDropping = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NOT NULL DEFAULT 'BMW'::character varying,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL DEFAULT 5,\n" +
                "  autointf integer NOT NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6,\n" +
                "  CONSTRAINT car_pkey PRIMARY KEY (id),\n" +
                "  CONSTRAINT unique_name UNIQUE (name),\n" +
                "  CONSTRAINT distfk FOREIGN KEY (user_id, user_name) REFERENCES users(id, name) ON UPDATE CASCADE ON DELETE CASCADE,\n" +
                "  CONSTRAINT car_check CHECK (((value > 50) AND (autointf < 100)))\n" +
                ");\n" +
                "CREATE UNIQUE INDEX car_pkey ON test_schema.car USING btree (id);\n" +
                "CREATE UNIQUE INDEX unique_name ON test_schema.car USING btree (name);\n" +
                "CREATE INDEX car_upper_idx ON test_schema.car USING btree (upper((name)::text));\n" +
                "CREATE UNIQUE INDEX car_idx ON test_schema.car USING btree (name);\n";

        String ddlAfterDropping = "CREATE TABLE test_schema.car (\n" +
                "  id integer NULL,\n" +
                "  name character varying(30) NULL,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NULL,\n" +
                "  autointf integer NULL,\n" +
                "  autoinccol integer NULL\n" +
                ");\n";

        //Поменяется только порядок индексов (поэтому не учитываю их, так как 24 перестановки)
        String ddlAfterRestoring = "CREATE TABLE test_schema.car (\n" +
                "  id integer NOT NULL,\n" +
                "  name character varying(30) NOT NULL DEFAULT 'BMW'::character varying,\n" +
                "  user_name character varying(30) NULL,\n" +
                "  user_id integer NULL,\n" +
                "  value integer NOT NULL DEFAULT 5,\n" +
                "  autointf integer NOT NULL DEFAULT 5,\n" +
                "  autoinccol integer NOT NULL DEFAULT 6,\n" +
                "  CONSTRAINT car_pkey PRIMARY KEY (id),\n" +
                "  CONSTRAINT unique_name UNIQUE (name),\n" +
                "  CONSTRAINT distfk FOREIGN KEY (user_id, user_name) REFERENCES users(id, name) ON UPDATE CASCADE ON DELETE CASCADE,\n" +
                "  CONSTRAINT car_check CHECK (((value > 50) AND (autointf < 100)))\n" +
                ");\n";

        String ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);

        Assert.assertTrue(ddl.equals(ddlBeforeDropping));


        pcm.getAndInitAllConstraints("test_schema", "car");

        pcm.dropAllConstraintsInTable("test_schema", "car", true,
                ConstraintType.CHECK, ConstraintType.DEFAULT, ConstraintType.FK, ConstraintType.PK,
                ConstraintType.INDEX, ConstraintType.NOT_NULL, ConstraintType.UNIQUE);


        ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);

        Assert.assertTrue(ddl.equals(ddlAfterDropping));

        pcm.restoreAllConstraintsInTable("test_schema", "car", true);

        ddl = jdbcTemplate.queryForObject("Select * from PostgresDDLAutoGenerationFunction('test_schema', 'car')", String.class);

        Assert.assertTrue(ddl.contains(ddlAfterRestoring) );

    }

    DataSource dataSource(){
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/TestDB");
        ds.setUsername("postgres");
        ds.setPassword("root");
        return ds;
    }
}
