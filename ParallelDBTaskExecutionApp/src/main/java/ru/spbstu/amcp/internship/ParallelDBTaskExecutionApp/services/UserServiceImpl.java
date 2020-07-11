package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services;

import org.springframework.jdbc.support.xml.SqlXmlFeatureNotImplementedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ITxAction;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TxAction;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.extra.PDataSourceTransactionManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDao;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.model.User;

import java.sql.SQLDataException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class UserServiceImpl implements UserService {

    private UserDao dao;
    private TransactionTemplate transactionTemplate;
    private TransactionTemplate transactionTemplate2;
    private PlatformTransactionManager mytransactionManager;

    public UserServiceImpl(UserDao dao, PDataSourceTransactionManager transactionManager){
        this.dao = dao;
        mytransactionManager = transactionManager;
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate2 = new TransactionTemplate(transactionManager);
    }

    public void createUser(int id, String name){
        dao.insert(new User(id, name));
    }

    @Transactional(rollbackFor = Exception.class)
    public void userTestNonParallelDeclarativeTransaction() throws Exception {

            dao.insert(new User(2, "Test"));
            dao.insert(new User(5,"Petr"));
            dao.changeUserName(5, "Ivan");
            dao.insert(new User(6,"John"));
            dao.deleteById(2);
            throw new Exception("roll back");

    }

    public void userTestNonParallelImperativeTransaction() throws Exception {

        System.out.println(transactionTemplate);
        transactionTemplate.execute(status -> {
            Object save = new Object();
            try {
                dao.insert(new User(7, "Test"));
                dao.insert(new User(8, "Petr"));
                save = status.createSavepoint();
                dao.changeUserName(9, "Ivan");
                dao.insert(new User(10, "John"));
                dao.deleteById(2);
                status.rollbackToSavepoint(save);
            }
            catch (Exception ex){
                status.setRollbackOnly();
            }finally {
                status.releaseSavepoint(save);
            }

            return null;
        });

    }

    public void exceptionTransaction(){
        transactionTemplate.execute(status->{

            dao.insert(new User(17, "After Exception"));
            dao.insert(new User(16, "Before Exception"));

            return null;
        });
    }

    public void myTx(){

        ConcurTxManager ctxm = new ConcurTxManager(transactionTemplate);
        ctxm.executeConcurTx(()->{
            dao.insert(new User(4, "Begin of ConcurTxTransaction"));

            new TxAction(ctxm).startAction(()->{
                dao.insert(new User(5, "Begin of innerAction"));
                return null;
            }).putAnotherAction(prev->{
                dao.insert(new User(6, "Next action1 in inner"));
                dao.insert(new User(7, "Next action2 in inner"));

                return null;
            }).putAnotherAction(prev->{

                //Откатится вся транзакция
                dao.insert(new User(7, "Next action3 in inner"));
                return null;
            });

            return null;
        });

        new ConcurTxManager(mytransactionManager).executeConcurTx(()->{
            dao.insert(new User(15, "Other ConcurTxTransaction2"));
            return null;
        });


    }

    private volatile Integer numberOfActiveChildThreads = 0;

    public void userTestParallelImperativeTransaction() throws Exception {

        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        //Начало транзакции
        transactionTemplate.execute(status -> {
            //SavePoints
            Object save = new Object();
            AtomicReference<Object> externalSavePoint = new AtomicReference<>(new Object());

            try {
                dao.insert(new User(1, "User"));
                TransactionSynchronizationManager.setCurrentTransactionName("First Transaction");

                dao.changeUserName(3, "User0");
                dao.insert(new User(10, "User10"));
                dao.deleteById(1);

                //Сохраняю ThreadLocal переменные
                List<Object> props = new ArrayList<Object>();
                props.add(TransactionSynchronizationManager.getResourceMap());
                props.add(TransactionSynchronizationManager.getSynchronizations());
                props.add(TransactionSynchronizationManager.getCurrentTransactionName());
                props.add(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
                props.add(TransactionSynchronizationManager.isActualTransactionActive());

                Callable<Object> r = () -> {

                    //Получаю ThreadLocal переменные TransactionSynchronizationManager родительского потока
                    try {

                        List<Object> secprops = new ArrayList<>();
                        secprops.addAll(props);

                        //Здесь передаю подключение к БД от родительского потока
                        Map<Object, Object> resources = (Map<Object, Object>) secprops.get(0);

                        List<TransactionSynchronization> syncs = (List<TransactionSynchronization>) secprops.get(1);
                        String txName = (String) secprops.get(2);
                        Integer isoLevel = (Integer) secprops.get(3);
                        Boolean active = (Boolean) secprops.get(4);

                        for (var e : resources.entrySet()) {
                            TransactionSynchronizationManager.bindResource(e.getKey(), e.getValue());
                        }
                        for (var e : syncs) {
                            TransactionSynchronizationManager.registerSynchronization(e);
                        }
                        TransactionSynchronizationManager.setCurrentTransactionName(txName);
                        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(isoLevel);
                        TransactionSynchronizationManager.setActualTransactionActive(active);

                    }catch (Exception e){
                        System.out.println(e.getMessage());
                    }

                    try {
                        System.out.println("Child thread isolation:" +  TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
                        System.out.println("Child thread name:" + TransactionSynchronizationManager.getCurrentTransactionName());
                        System.out.println("Is child alive:" + TransactionSynchronizationManager.isActualTransactionActive());

                        dao.insert(new User(11, "User11"));
                        dao.insert(new User(15, "User15"));

                        //SavePoint дочернего потока
                        Object saveInOtherThread = status.createSavepoint();
                        for (int i = 12; i < 15; i++) {
                            dao.insert(new User(i, "SomeNewName in Thread 2" + Thread.currentThread().getName()));
                        }
                        //Откат в дочернем потоке на том же подключении к БД, что и у родителя
                        status.rollbackToSavepoint(saveInOtherThread);
                        status.releaseSavepoint(saveInOtherThread);

                        dao.insert(new User(16, "Before External"));
//                        dao.insert(new User(16, "Before External2"));

                        externalSavePoint.set(status.createSavepoint());

                        dao.insert(new User(17, "After External"));

                        System.out.println("DONE!");

                    } catch (RuntimeException e) {
                        System.out.println(status.isCompleted());
                        System.out.println(e.getMessage());
                        System.out.println("Unchecked in Child thread");
                    }finally {
                        System.out.println("Finished child thread: " + Thread.currentThread().getName());
                    }

                    //Оповещаем родительский поток, что можно сделать commit

                    synchronized (this) {
                        numberOfActiveChildThreads--;
                        this.notifyAll();
                    }

                    return null;
                };
                //Запуск дочернего потока в рамках одной транзакции
                synchronized(this) {
                    numberOfActiveChildThreads++;
                }

                new Thread(new FutureTask(r)).start();

                //Родительский поток не будет делать commit, пока canCommit > 0
                while (numberOfActiveChildThreads != 0){
                    synchronized (this) { this.wait(); }
                }

                dao.changeUserName(10, "Before Roll Back");
                //Для примера ролбэк по сэйвпоинту не из потока
                status.rollbackToSavepoint(externalSavePoint.get());
                status.releaseSavepoint(externalSavePoint.get());

                System.out.println("Commit?: " + Thread.currentThread().getName());

            }catch (RuntimeException | InterruptedException e){
                System.out.println("Error" + e.getMessage());
            }  finally {
                if(status.hasSavepoint()) {
                    status.releaseSavepoint(save);
                }
            }
            return null;
        });
    }
}
