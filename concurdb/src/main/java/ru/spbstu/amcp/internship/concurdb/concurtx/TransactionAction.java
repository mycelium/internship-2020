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
 * Class whose instances can form a chain of tasks
 * (both asynchronous and sequential) as part of a transaction,
 * created by an instance of the ConcurrentTransactionManager class.
 *
 */

@Component
public class TransactionAction implements ITransactionAction {

    /**
     * Managing asynchronous and sequential
     * task chains.
     */
    private ConcurrentTransactionManager concurrentTransactionManager;
    /**
     * A list of single thread pools, each of which executes
     * a  chain of tasks. When creating a new sequential task chain
     * a single thread pool is created.
     */
    private List<ExecutorService> executorServices = new ArrayList<>();

    /**
     * A property that is necessary in order to keep track of
     * the process of executing chains of tasks within an object and not
     * make a premature commit in the parent thread.
     */
    private CompletableFuture<?> completableFuture = null;

    /**
     * When creating a new object, it is necessary to form
     * at least one single thread pool to execute the chain of
     * sequential tasks.
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
     * Starts the first chain of sequential tasks and transfers
     * all transaction properties to the thread from the pool.
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
     *  Adds another task to the sequential chain for future execution within the same thread from the pool
     */
    @Override
    public TransactionAction putAnotherAction(Function<? super Object, ?> action) {
        //Транзакция останется действительной
        completableFuture = completableFuture.thenApplyAsync(action,
                executorServices.get(executorServices.size()-1));
        return this;
    }

    /**
     * Adds one new task to a new sequential chain running already in another thread from the new pool.
     * All transaction properties are transferred to the new thread.
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
     * Blocks the thread before the completion of all created task chains.
     * Turns off each created pool.
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
     * Sets the properties of the transaction in the child thread.
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