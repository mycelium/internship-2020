package ru.spbstu.amcp.internship.concurdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.ConstraintType;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.SQLiteConstraintsManager;

import javax.sql.DataSource;
import java.util.*;

@SpringBootTest
public class SQliteTest {

    SQLiteConstraintsManager scm;

    JdbcTemplate jdbcTemplate;

    List<List<String>> generatedIndexes = Arrays.asList(
            Arrays.asList("trackindex1", "CREATE INDEX main.trackindex1 ON track(trackartist, trackid)"),
            Arrays.asList("trackindex2", "CREATE INDEX main.trackindex2 ON track(trackartist DESC, trackid)"),
            Arrays.asList("unitrack", "CREATE UNIQUE INDEX main.unitrack ON track(trackartist, trackid)"),
            Arrays.asList("partial_index", "CREATE INDEX main.partial_index ON track(trackid) WHERE trackname IS NOT NULL"),
            Arrays.asList("partial_unique_index", "CREATE UNIQUE INDEX main.partial_unique_index ON track(trackid) WHERE trackname IS NOT NULL")
    );

    @Before
    public void init(){
        jdbcTemplate = new JdbcTemplate(dataSource());
        scm = new SQLiteConstraintsManager(jdbcTemplate);

        Resource resource = new ClassPathResource("sqlitecar.sql");
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.execute(jdbcTemplate.getDataSource());
    }



    @Test
    public void testIndexInitialization() throws InterruptedException {

        List<Constraint> indexes = scm.getAndInitAllConstraints("main", "track");

        Assert.assertTrue(indexes.size() == generatedIndexes.size());

        indexes.forEach(
                constraint -> {
                    Assert.assertTrue(generatedIndexes.stream().anyMatch(nameAndDDL ->
                            nameAndDDL.get(0).equals(constraint.getIndexName()) && nameAndDDL.get(1).equals(constraint.getRestoreDDL())));
                }
        );
    }


    @Test
    public void testDropAndRestorePartialUniqueIndex(){

        List<List<String>> expectedResultAfterDropping = Arrays.asList(
                Arrays.asList("trackindex1", "CREATE INDEX main.trackindex1 ON track(trackartist, trackid)"),
                Arrays.asList("trackindex2", "CREATE INDEX main.trackindex2 ON track(trackartist DESC, trackid)"),
                Arrays.asList("unitrack", "CREATE UNIQUE INDEX main.unitrack ON track(trackartist, trackid)"),
                Arrays.asList("partial_index", "CREATE INDEX main.partial_index ON track(trackid) WHERE trackname IS NOT NULL")
        );

        scm.getAndInitAllConstraints("main", "track");

        scm.dropOneConstraint("main", "track", "partial_unique_index", ConstraintType.INDEX);


        List<List<String>> obtainedAfterDropping = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterDropping.size(), expectedResultAfterDropping.size());

        obtainedAfterDropping.stream().forEach(ob -> {

            Assert.assertTrue(expectedResultAfterDropping.stream().anyMatch(nameAndDDL ->
                 nameAndDDL.get(0).equals(ob.get(0)) && nameAndDDL.get(1).equals(ob.get(1))
            ));

        });

        scm.restoreOneConstraint("main", "track", "partial_unique_index", ConstraintType.INDEX, true);

        List<List<String>> obtainedAfterRestoring = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterRestoring.size(), generatedIndexes.size());

        obtainedAfterRestoring.stream().forEach(ob -> {

            Assert.assertTrue(generatedIndexes.stream().anyMatch(nameAndDDL ->
                    nameAndDDL.get(0).equals(ob.get(0)) && nameAndDDL.get(1).equals(ob.get(1))
            ));

        });

    }


    @Test
    public void testDropAndRestoreAllIndexes(){

        scm.getAndInitAllConstraints("main", "track");

        scm.dropAllConstraintsInTable("main", "track", true,  ConstraintType.INDEX);

        List<List<String>> obtainedAfterDropping = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterDropping.size(), 0);

        scm.restoreAllConstraintsInTable("main", "track", true);

        List<List<String>> obtainedAfterRestoring = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterRestoring.size(), generatedIndexes.size());

        obtainedAfterRestoring.stream().forEach(ob -> {

            Assert.assertTrue(generatedIndexes.stream().anyMatch(nameAndDDL ->
                    nameAndDDL.get(0).equals(ob.get(0)) && nameAndDDL.get(1).equals(ob.get(1))
            ));

        });

    }


    public DataSource dataSource(){
        SQLiteConfig config = new SQLiteConfig();
        config.enforceForeignKeys(true);
        SQLiteDataSource ds = new SQLiteDataSource(config);
        ds.setUrl("jdbc:sqlite:sql.db");
        return ds;
    }



    static  List<List<String>> getIndexes(String schemaName, String tableName, JdbcTemplate jdbc) {

        List<List<String>> result = new ArrayList<>();

        List<String> indexNames = jdbc.query("PRAGMA "+schemaName+".index_list('"+tableName+"');",
                (rs, i) -> {
                    return rs.getString("name");
                });

        indexNames.stream().forEach(indexName -> {

            String ddl = jdbc.queryForObject("SELECT sql FROM "+schemaName+".sqlite_master\n" +
                    " where type = 'index' and tbl_name = '"+tableName+"'\n" +
                    "  and name = '"+indexName+"'", String.class);

            ddl = ddl.replaceFirst("INDEX " +indexName+ "( .*)", "INDEX " +schemaName+"."+indexName+ "$1");

            result.add(Arrays.asList(indexName, ddl));

        });

        return result;
    }


}
