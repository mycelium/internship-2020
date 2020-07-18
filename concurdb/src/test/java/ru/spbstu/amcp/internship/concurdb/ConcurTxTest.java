package ru.spbstu.amcp.internship.concurdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.dao.UserDao;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.dao.UserDaoImpl;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.model.User;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.services.UserServiceImpl;
import ru.spbstu.amcp.internship.concurdb.concurtx.ConcurrentTransactionManager;
import ru.spbstu.amcp.internship.concurdb.concurtx.TransactionRollbackPolicy;
import ru.spbstu.amcp.internship.concurdb.concurtx.TransactionAction;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;


@SpringBootTest
public class ConcurTxTest {

    UserServiceImpl service;
    UserDao dao;
    DataSourceTransactionManager transactionManager;
    JdbcTemplate jdbcTemplate;

    //Schema - public, table - users5
    DataSource dataSource(){
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/TestDB");
        ds.setUsername("postgres");
        ds.setPassword("root");
        return ds;
    }

    @Before
    public void init(){
        DataSource dataSource = dataSource();
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        dao = new UserDaoImpl(new JdbcTemplate(dataSource));
        service = new UserServiceImpl(dao, transactionManager);
    }



    @Test
    public void innerSavePointTest() {
        List<User> expectedResult = Arrays.asList(
                new User(5, "Begin of innerAction"),
                new User(6, "Next action in inner"),
                new User(7, "Next action in inner"),
                new User(8, "Next action in innerAsync"),
                new User(9, "Next action in innerAsync"),
                new User(10, "Next action in innerAsync"),
                new User(55, "Next action")
                );

        ConcurrentTransactionManager ctxm = new ConcurrentTransactionManager(service.getTransactionTemplate());
        ctxm.setTransactionRollbackPolicy(TransactionRollbackPolicy.ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD);
        ctxm.getTransactionTemplate().setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        ctxm.getTransactionTemplate().setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        ctxm.executeConcurrentTransaction(() -> {

            new TransactionAction(ctxm).startAction(() -> {
                dao.insert(new User(5, "Begin of innerAction"));
                return null;
            })
                    .putAnotherAction(prev -> {
                        dao.insert(new User(6, "Next action in inner"));
                        dao.insert(new User(7, "Next action in inner"));
                        return null;
                    })
                    .putAnotherActionAsync(prev -> {
                        Object savePoint = new Object();
                        try {
                            savePoint = ctxm.createSavepoint();
                            dao.insert(new User(7, "Next action in innerAsync"));
                        } catch (Exception e) {
                            System.out.println(e.getMessage());

                            ctxm.rollbackToSavepoint(savePoint);
                        } finally {
                            ctxm.releaseSavepoint(savePoint);
                        }
                        dao.insert(new User(8, "Next action in innerAsync"));
                        dao.insert(new User(9, "Next action in innerAsync"));
                        dao.insert(new User(10, "Next action in innerAsync"));
                        return null;
                    })
                    .putAnotherActionAsync(prev -> {
                        dao.insert(new User(55, "Next action"));

                        Object savePoint1 = ctxm.createSavepoint();
                        TransactionAction inneraction = new TransactionAction(ctxm).startAction(() -> {
                            dao.insert(new User(66, "Inner in Inner"));
                            return null;
                        }).putAnotherActionAsync(prevRes -> {
                            dao.insert(new User(77, "Async Inner"));
                            return null;
                        });
                        try {
                            inneraction.get();
                        } catch (ExecutionException exception) {
                            exception.printStackTrace();
                        }
                        ctxm.rollbackToSavepoint(savePoint1);
                        return null;
                    });

            return null;
        });

        List<User> obtainedResult = jdbcTemplate.query("select * from users5",
                (rs, i) -> {
                    return new User(rs.getInt("id"), rs.getString("name"));
                });

        Assert.assertTrue(obtainedResult.size() == expectedResult.size());

        for (int i = 0; i < obtainedResult.size(); i++) {

            Assert.assertTrue(obtainedResult.get(i).getId() == expectedResult.get(i).getId());
            Assert.assertTrue(obtainedResult.get(i).getName().equals(expectedResult.get(i).getName()));

        }

        ctxm.shutdownEveryExecutor();
    }

        @Test
        public void outerSavePointTest() {

            List<User> expectedResult = Arrays.asList(
                    new User(100, "New Transaction"),
                    new User(101, "New transaction - Parent Action")
            );

            ConcurrentTransactionManager cxtm = new ConcurrentTransactionManager(service.getTransactionTemplate());
            cxtm.executeConcurrentTransaction(() -> {
                try {
                    try {
                        cxtm.executeConcurrentTransaction(() -> {
                            return null;
                        });
                    } catch (CannotCreateTransactionException e) {
                        System.out.println(e.getMessage());
                    }

                    dao.insert(new User(100, "New Transaction"));

                    AtomicReference<Object> savePoint = new AtomicReference<>(new Object());

                    TransactionAction parenttx = new TransactionAction(cxtm).startAction(() -> {
                        dao.insert(new User(101, "New transaction - Parent Action"));

                        savePoint.set(cxtm.createSavepoint());

                        TransactionAction childtx = new TransactionAction(cxtm).startAction(() -> {

                            dao.insert(new User(102, "New transaction - Child Action"));
                            return null;

                        }).putAnotherActionAsync(res -> {

                            dao.insert(new User(103, "New transaction - Child Action"));
                            dao.changeUserName(100, "New name");

                            return res;
                        });

                        TransactionAction childtx2 = new TransactionAction(cxtm).startAction(() -> {
                            dao.insert(new User(104, "New transaction - Child Action"));
                            return null;
                        });

                        try {
                            childtx.get();
                            childtx2.get();
                        } catch (ExecutionException exception) {
                            exception.printStackTrace();
                        }
                        return null;
                    });

                    parenttx.get();
                    cxtm.rollbackToSavepoint(savePoint.get());
                    Thread.sleep(100);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                return null;
            });

            List<User> obtainedResult = jdbcTemplate.query("select * from users5",
                    (rs, i) -> {
                        return new User(rs.getInt("id"), rs.getString("name"));
                    });

            Assert.assertTrue(obtainedResult.size() == expectedResult.size());

            for (int i = 0; i < obtainedResult.size(); i++) {

                Assert.assertTrue(obtainedResult.get(i).getId() == expectedResult.get(i).getId());
                Assert.assertTrue(obtainedResult.get(i).getName().equals(expectedResult.get(i).getName()));

            }
            cxtm.shutdownEveryExecutor();

        }


        @Test
        public void nestedTransactionTest(){

            List<User> expectedResult = Arrays.asList(
                    new User(200, "First Tx")
            );

        ConcurrentTransactionManager ctxm1 = new ConcurrentTransactionManager(new TransactionTemplate(transactionManager));
        ConcurrentTransactionManager ctxm2 = new ConcurrentTransactionManager(new TransactionTemplate(transactionManager));

        ctxm1.executeConcurrentTransaction(()->{

            dao.insert(new User(200, "First Tx"));

            CompletableFuture.runAsync(() -> {
                ctxm2.setTransactionRollbackPolicy(TransactionRollbackPolicy.ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD);
                ctxm2.executeConcurrentTransaction(() -> {

                    dao.insert(new User(201, "Second Tx"));
                    new TransactionAction(ctxm2).startAction(() -> {
                        dao.insert(new User(202, "Second Tx"));
                        throw new RuntimeException("Rollback Second Tx");

                    });
                    return null;
                });
            });

            return null;
        });

            List<User> obtainedResult = jdbcTemplate.query("select * from users5",
                    (rs, i) -> {
                        return new User(rs.getInt("id"), rs.getString("name"));
                    });

            Assert.assertTrue(obtainedResult.size() == expectedResult.size());

            for (int i = 0; i < obtainedResult.size(); i++) {

                Assert.assertTrue(obtainedResult.get(i).getId() == expectedResult.get(i).getId());
                Assert.assertTrue(obtainedResult.get(i).getName().equals(expectedResult.get(i).getName()));

            }
            ctxm1.shutdownEveryExecutor();
            ctxm2.shutdownEveryExecutor();

    }

}
