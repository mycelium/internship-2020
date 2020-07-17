package ru.spbstu.amcp.internship.ParallelDBTaskExecution;

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
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.ConstraintType;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.SQLiteConstraintsManager;

import javax.sql.DataSource;
import java.util.*;

@SpringBootTest
public class SQliteTest {

    SQLiteConstraintsManager scm;

    JdbcTemplate jdbcTemplate;

    //Все сгенерированные индексы из тестового sql файла
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

        //Загрузка тестового sql файла
        Resource resource = new ClassPathResource("sqlitecar.sql");
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.execute(jdbcTemplate.getDataSource());
    }


    /***
     *  Тест проверяет, что все индексы прочитаны
     */
    @Test
    public void testIndexInitialization() throws InterruptedException {

        //Получаю все индексы
        List<Constraint> indexes = scm.getAndInitAllConstraints("main", "track");

        //Проверяю, что получены все индексы
        Assert.assertTrue(indexes.size() == generatedIndexes.size());

        //Проверяю ddl для каждого индекса
        indexes.forEach(
                constraint -> {
                    Assert.assertTrue(generatedIndexes.stream().anyMatch(nameAndDDL ->
                            nameAndDDL.get(0).equals(constraint.getIndexName()) && nameAndDDL.get(1).equals(constraint.getRestoreDDL())));
                }
        );
    }

    /**
     * Тест на удаления одного индекса
     */
    @Test
    public void testDropAndRestorePartialUniqueIndex(){

        //Ожидаемый результат после удаления индекса
        List<List<String>> expectedResultAfterDropping = Arrays.asList(
                Arrays.asList("trackindex1", "CREATE INDEX main.trackindex1 ON track(trackartist, trackid)"),
                Arrays.asList("trackindex2", "CREATE INDEX main.trackindex2 ON track(trackartist DESC, trackid)"),
                Arrays.asList("unitrack", "CREATE UNIQUE INDEX main.unitrack ON track(trackartist, trackid)"),
                Arrays.asList("partial_index", "CREATE INDEX main.partial_index ON track(trackid) WHERE trackname IS NOT NULL")
        );

        //Получаю все индексы
        scm.getAndInitAllConstraints("main", "track");

        //Удаляю индекс
        scm.dropOneConstraint("main", "track", "partial_unique_index", ConstraintType.INDEX);


        //Проверяю, что остались нужные индексы
        List<List<String>> obtainedAfterDropping = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterDropping.size(), expectedResultAfterDropping.size());

        obtainedAfterDropping.stream().forEach(ob -> {

            Assert.assertTrue(expectedResultAfterDropping.stream().anyMatch(nameAndDDL ->
                 nameAndDDL.get(0).equals(ob.get(0)) && nameAndDDL.get(1).equals(ob.get(1))
            ));

        });

        //Восстанавливаю индекс
        scm.restoreOneConstraint("main", "track", "partial_unique_index", ConstraintType.INDEX, true);

        //Проверяю, что восстановлены все индексы
        List<List<String>> obtainedAfterRestoring = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterRestoring.size(), generatedIndexes.size());

        obtainedAfterRestoring.stream().forEach(ob -> {

            Assert.assertTrue(generatedIndexes.stream().anyMatch(nameAndDDL ->
                    nameAndDDL.get(0).equals(ob.get(0)) && nameAndDDL.get(1).equals(ob.get(1))
            ));

        });

    }

    /**
     * Тест на удаления всех индексов
     */
    @Test
    public void testDropAndRestoreAllIndexes(){

        //Получаю все индексы
        scm.getAndInitAllConstraints("main", "track");

        //Удаляю все индексы
        scm.dropAllConstraintsInTable("main", "track", true,  ConstraintType.INDEX);

        //Проверяю, что индексов больше нет
        List<List<String>> obtainedAfterDropping = getIndexes("main", "track", jdbcTemplate);

        Assert.assertEquals(obtainedAfterDropping.size(), 0);

        //Восстанавливаю индексы
        scm.restoreAllConstraintsInTable("main", "track", true);

        //Проверяю, что восстановлены все индексы
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
        //Обязательно включить FK!
        config.enforceForeignKeys(true);
        SQLiteDataSource ds = new SQLiteDataSource(config);
        ds.setUrl("jdbc:sqlite:sql.db;foreign keys=true;");
        return ds;
    }


    /**
     * Метод возвращает список элементов вида (имя индекса, его DDL код), полученных из системных таблиц SQLite
     */
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
