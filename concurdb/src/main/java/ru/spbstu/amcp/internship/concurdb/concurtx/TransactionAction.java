package ru.spbstu.amcp.internship.concurdb.concurtx;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Класс, объекты которого могут формировать цепочку задач
 * (как асинхронных, так и последовательных) в рамках транзакции,
 * созданной объектом класса ConcurrentTransactionManager.
 *
 * Основая идея работы заключается в запуске асинхронных
 * и последовательных задач с помощью
 * CompletableFuture. Однако CompletableFuture может выполнять
 * действие из thenApply не в том потоке, который выполнял до этого
 * supplyAsync. Поэтому решено в данном классе для вызова как асинхронных,
 * так и последовательных задач использовать thenApplyAsync с явным указанием
 * потока (на самом деле пула потоков из одного потока), который будет
 * выполнять действие из thenApplyAsync.
 */

@Component
public class TransactionAction implements ITransactionAction {

    /**
     * Менеджер транзакции, управляющий асинхронными и последовательными
     * цепочками задач.
     */
    private ConcurrentTransactionManager concurrentTransactionManager;
    /**
     * Список пулов из одного потока, каждый из которых выполняет
     * последовательную цепочку задач. При создании в другом потоке
     * новой последовательной цепочки задач (асинхронной)
     * создаётся пул из одного потока.
     */
    private List<ExecutorService> executorServices = new ArrayList<>();

    /**
     * Свойство, которое необходимо для того, чтобы следить за
     * процессом выполнения цепочек задач в рамках объекта TransactionAction
     * и не делать преждевременный коммит в родительском потоке.
     */
    private CompletableFuture<?> completableFuture = null;

    /**
     * При создании нового объекта необходимо сформировать
     * хотя бы один пул из одного потока для выполнения цепочки
     * последовательных задач.
     * @param concurrentTransactionManager
     */
    public TransactionAction(ConcurrentTransactionManager concurrentTransactionManager) {
        if(!concurrentTransactionManager.isActiveTransaction()){
            throw new RuntimeException("Can't create TransactionAction outside transaction");
        }
        this.concurrentTransactionManager = concurrentTransactionManager;
        executorServices.add(Executors.newSingleThreadExecutor());
        concurrentTransactionManager.putChildTxAction(this);
    }

    /**
     * Запуск первой цепочки последовательных задач с передачей
     * всех свойств транзакции в поток из пула.
     * @param action
     * @return
     */
    @Override
    public TransactionAction startAction(Supplier<? extends Object> action) {
        completableFuture = CompletableFuture.supplyAsync(()->{
            //Передача свойств транзакции дочернему потоку
            setTransactionProperties();
            return action.get();
        }, executorServices.get(0));
        return this;
    }

    /**
     *    Метод добавляет ещё одну задачу в последовательную цепочку
     *    для будущего выполнения в рамках того же потока из пула
     */
    @Override
    public TransactionAction putAnotherAction(Function<? super Object, ?> action) {
        //Транзакция останется действительной
        completableFuture = completableFuture.thenApplyAsync(action,
                executorServices.get(executorServices.size()-1));
        return this;
    }

    /**
     * Метод добавляет одну задачу в новую последовательную цепочку, выполняющуюся
     * уже в другом потоке из нового пула. В новый поток передаются все свойства
     * транзакции.
     * @param action
     * @return
     */
    @Override
    public TransactionAction putAnotherActionAsync(Function<? super Object, ?> action) {
        //Добавляется новый исполнитель явно для CompletableFuture, чтобы
        //запустить выполнение в том потоке, который необходим
        executorServices.add(Executors.newSingleThreadExecutor());

        completableFuture = completableFuture.thenApplyAsync(
                previousResult->{
                    //Передача свойств транзакции дочернему потоку
                    setTransactionProperties();
                    return action.apply(previousResult);
                },
                executorServices.get(executorServices.size()-1));
        return this;
    }


    /**
     * Метод блокирует поток, его вызвавший до завершения всех
     * созданных цепочек задач.
     * В конце необходимо выключить каждый созданный пул.
     * @return результат выполнения последней задачи
     */
    public Object get() throws ExecutionException {
        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw e;
        }finally {
            //Закрываем все однопоточные исполнители
            for (var executor: executorServices) {
                if(!executor.isShutdown()) {
                    executor.shutdown();
                    System.out.println("Executor shutdown");
                }
            }
        }
        return null;
    }

    /**
     * Метод устанавливает свойства транзакции в дочернем потоке
     */
    private void setTransactionProperties(){
        List<Object> props = new ArrayList<>();
        props.addAll(concurrentTransactionManager.getTransactionProperties());

        //Здесь передаю подключение к БД от родительского потока
        Map<Object, Object> resources = (Map<Object, Object>) props.get(0);
        List<TransactionSynchronization> syncs = (List<TransactionSynchronization>) props.get(1);
        String txName = (String) props.get(2);
        Integer isoLevel = (Integer) props.get(3);
        Boolean active = (Boolean) props.get(4);

        for (var e : resources.entrySet()) {
            TransactionSynchronizationManager.bindResource(e.getKey(), e.getValue());
        }
        for (var e : syncs) {
            TransactionSynchronizationManager.registerSynchronization(e);
        }
        TransactionSynchronizationManager.setCurrentTransactionName(txName);
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(isoLevel);
        TransactionSynchronizationManager.setActualTransactionActive(active);
    }

}
