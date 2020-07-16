package ru.spbstu.amcp.internship.ParallelDBTaskExecution;

import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.dao.UserDao;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.dao.UserDaoImpl;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.model.User;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.services.UserServiceImpl;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TransactionRollbackPolicy;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TxAction;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.MariaDBConstraintsManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.extra.PDataSourceTransactionManager;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;


@SpringBootTest
public class ConcurTxTest {

    UserServiceImpl service;
    UserDao dao;
    PDataSourceTransactionManager transactionManager;

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
        transactionManager = new PDataSourceTransactionManager(dataSource);
        dao = new UserDaoImpl(new JdbcTemplate(dataSource));
        service = new UserServiceImpl(dao, transactionManager);
    }

    @Test
    public void myTx(){

        //Создаю менеджер параллелльной транзакции
        ConcurTxManager ctxm = new ConcurTxManager(service.getTransactionTemplate());

        //Устанавливаю политику отката - если в каком-то потоке необработанный exception, то вся транзакция
        //откатится
        ctxm.setTxpolicy(TransactionRollbackPolicy.ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD);

        //Можно установить уровень изоляции транзакции
        ctxm.getTransactionTemplate().setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        //Можно установить propagation
        ctxm.getTransactionTemplate().setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        //Запускаю транзакцию
        ctxm.executeConcurTx(()->{

            //Создаю последовательность задач в транзакции
            //Аналогично - CompletableFuture, но с передачей thread local
            //переменных транзакции дочерним потокам.

            new TxAction(ctxm).startAction(()->{
                //DAO записывает в БД нового пользователя
                //Этот юзер будет записан
                dao.insert(new User(5, "Begin of innerAction"));
                return null;
            })
                    //Ставлю в очередь новую задачу для исполнения в том же потоке.
                    //Можно передавать результат предыдущей задачи новой задаче в цепочке.
                    .putAnotherAction(prev->{

                        //Эти юзеры записаны будут
                        dao.insert(new User(6, "Next action in inner"));
                        dao.insert(new User(7, "Next action in inner"));

                        return null;
                    })
                    //Эта задача запустится в новом потоке
                    .putAnotherActionAsync(prev->{
                        Object savePoint = new Object();
                        try {
                            //Ручной откат поддерживается в дочерних потоках
                            savePoint = ctxm.createSavepoint();

                            //Юзер заново не перезапишется и транзакция не упадёт
                            dao.insert(new User(7, "Next action in innerAsync"));
                        }catch (Exception e){
                            System.out.println(e.getMessage());

                            ctxm.rollbackToSavepoint(savePoint);
                        }finally {
                            ctxm.releaseSavepoint(savePoint);
                        }
                        //Эти юзеры записаны будут
                        dao.insert(new User(8, "Next action in innerAsync"));
                        dao.insert(new User(9, "Next action in innerAsync"));
                        dao.insert(new User(10, "Next action in innerAsync"));
                        return null;
                    })
                    //Эта задача запустится в новом потоке
                    .putAnotherActionAsync(prev->{
                        //Этот юзер тоже запишется
                        dao.insert(new User(55, "Next action"));

                        Object savePoint1 = ctxm.createSavepoint();
                        TxAction inneraction = new TxAction(ctxm).startAction(()->{
                            //Этот юзер не запишется
                            dao.insert(new User(66,"Inner in Inner"));
                            return null;
                        }).putAnotherActionAsync(prevRes->{
                            //Этот юзер не запишется
                            dao.insert(new User(77, "Async Inner"));
                            return null;
                        });
                        //Так как хочется, чтобы откатилась задача, созданная выше, то надо подождать,
                        //пока она выполнится
                        try {
                            inneraction.get();
                        } catch (ExecutionException exception) {
                            exception.printStackTrace();
                        }
                        ctxm.rollbackToSavepoint(savePoint1);

                        //ctxm.setRollbackOnly(); - ручной откат до начала транзакции - все юзеры будут стёрты
                        //throw new RuntimeException("Откат транзакции из-за выбранной политики") - все юзеры будут стёрты
                        return null;
                    });

            return null;
        });

        //Запустим новую транзакцию
        ConcurTxManager cxtm2 = new ConcurTxManager(service.getTransactionTemplate());
        cxtm2.executeConcurTx(()->{
            try {
                //Менеджер не запустит новую транзакцию внутри текущей для одного менеджера
                cxtm2.executeConcurTx(()->{return null;});

                //Можно не запускать задачи, а сразу использовать DAO
                //Этот юзер будет записан
                dao.insert(new User(100, "New Transaction"));

                AtomicReference<Object> savePoint = new AtomicReference<>(new Object());

                TxAction parenttx = new TxAction(cxtm2).startAction(()->{
                    //Этот юзер будет записан
                    dao.insert(new User(101, "New transaction - Parent Action"));

                    //Устнавливаю сейвпоинт в другом потоке
                    savePoint.set(cxtm2.createSavepoint());

                    //Задача - вложенность 2-го уровня
                    TxAction childtx = new TxAction(cxtm2).startAction(()->{

                        //Этот юзер не будет записан, так как он после сэйвпоинта
                        dao.insert(new User(102, "New transaction - Child Action"));

                        return null;
                    }).putAnotherActionAsync(res->{

                        //Этот юзер не будет записан, так как он после сэйвпоинта
                        dao.insert(new User(103, "New transaction - Child Action"));
                        //Изменение имени откатится
                        dao.changeUserName(100, "New name");

                        return res;
                    });

                    TxAction childtx2 = new TxAction(cxtm2).startAction(()->{

                        //Этот юзер не будет записан, так как он после сэйвпоинта
                        dao.insert(new User(104, "New transaction - Child Action"));
                        return  null;
                    });

                    try {
                        childtx.get();
                        childtx2.get();
                    } catch (ExecutionException exception) {
                        exception.printStackTrace();
                    }
                    return null;
                });

                //Ожидаем завершения всех подзадач в транзакции
                parenttx.get();

                //Делаем откат - останется Юзер с id = 101 и 100.
                cxtm2.rollbackToSavepoint(savePoint.get());

                Thread.sleep(100);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        //Вложенные транзакции

        ConcurTxManager ctxm3 = new ConcurTxManager(new TransactionTemplate(transactionManager));
        ConcurTxManager ctxm4 = new ConcurTxManager(new TransactionTemplate(transactionManager));

        ctxm3.executeConcurTx(()->{

            //Этот юзер будет записан
            dao.insert(new User(200, "First Tx"));

            //Для создания вложенной транзакции обязательно надо использовать новый поток (причина: thread local переменные)
            CompletableFuture.runAsync(() -> {
                ctxm4.setTxpolicy(TransactionRollbackPolicy.ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD);
                ctxm4.executeConcurTx(() -> {

                    //Этот юзер не будет записан, из-за выбранной политики
                    dao.insert(new User(201, "Second Tx"));

                    new TxAction(ctxm4).startAction(() -> {

                        //Этот юзер не будет записан, из-за выбранной политики
                        dao.insert(new User(202, "Second Tx"));

                        //Исключение вернет БД в состояние до транзакции - из-за выбранной политики
                        throw new RuntimeException("Rollback Second Tx");

                    });
                    return null;
                });
            });

            return null;
        });

        try{
            new TxAction(ctxm3).startAction(()->{

                return null;
            });
        }catch (RuntimeException e){
            System.out.println("expected exception");
        }

    }

}
