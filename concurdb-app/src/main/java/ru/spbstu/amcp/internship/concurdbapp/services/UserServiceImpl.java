package ru.spbstu.amcp.internship.concurdbapp.services;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.concurdb.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.concurdb.concurtx.TransactionRollbackPolicy;
import ru.spbstu.amcp.internship.concurdb.concurtx.TxAction;
import ru.spbstu.amcp.internship.concurdbapp.dao.UserDao;
import ru.spbstu.amcp.internship.concurdbapp.model.User;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class UserServiceImpl implements UserService {

    private UserDao dao;
    private TransactionTemplate transactionTemplate;
    private TransactionTemplate transactionTemplate2;
    private PlatformTransactionManager mytransactionManager;

    public UserServiceImpl(UserDao dao, DataSourceTransactionManager transactionManager){
        this.dao = dao;
        mytransactionManager = transactionManager;
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate2 = new TransactionTemplate(transactionManager);
    }

    //Пример работы API
    public void myTx(){

        //Создаю менеджер параллелльной транзакции
        ConcurTxManager ctxm = new ConcurTxManager(transactionTemplate);

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
        ConcurTxManager cxtm2 = new ConcurTxManager(transactionTemplate);
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

        ConcurTxManager ctxm3 = new ConcurTxManager(new TransactionTemplate(mytransactionManager));
        ConcurTxManager ctxm4 = new ConcurTxManager(new TransactionTemplate(mytransactionManager));

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


    @Transactional(rollbackFor = Exception.class)
    public void userTestNonParallelDeclarativeTransaction() throws Exception {

        dao.insert(new User(2, "Test"));
        dao.insert(new User(5,"Petr"));
        dao.changeUserName(5, "Ivan");
        dao.insert(new User(6,"John"));
        dao.deleteById(2);
        throw new Exception("roll back");

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
}
