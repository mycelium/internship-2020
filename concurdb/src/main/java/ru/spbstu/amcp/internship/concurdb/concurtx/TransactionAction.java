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
 */
@Component
public class TransactionAction implements ITransactionAction {

    /**
     * Managing asynchronous and sequential
     * task chains.
     */
    private ConcurrentTransactionManager concurrentTransactionManager;


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

        concurrentTransactionManager.addNewExecutor(this);
        concurrentTransactionManager.putChildTxAction(this);
    }

    /**
     * When creating a new object, it is necessary to form
     * at least one single thread pool to execute the chain of
     * sequential tasks.
     * @param concurrentTransactionManager
     */
    public TransactionAction(ConcurrentTransactionManager concurrentTransactionManager, ExecutorService executorService) {
        if(!concurrentTransactionManager.isActiveTransaction()){
            throw new RuntimeException("Can't create TransactionAction outside transaction");
        }
        this.concurrentTransactionManager = concurrentTransactionManager;

        concurrentTransactionManager.addNewExecutor(this, executorService);
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
        if(!concurrentTransactionManager.isActiveTransaction())
            throw new RuntimeException("Can't start action. Transaction is not active");

        completableFuture = CompletableFuture.supplyAsync(()->{

            if(!concurrentTransactionManager.isActiveTransaction())
                throw new RuntimeException("Can't run action. Transaction is not active");

            setTransactionProperties();
            return action.get();

        }, concurrentTransactionManager.getCurrentExecutor(this));

        return this;
    }



    /**
     *  Adds another task to the sequential chain for future execution within the same thread from the pool
     */
    @Override
    public TransactionAction putAnotherAction(Function<? super Object, ?> action) {
        if(!concurrentTransactionManager.isActiveTransaction())
            throw new RuntimeException("Can't add another action. Transaction is not active");

        completableFuture = completableFuture.thenApplyAsync(previousResult->{

                    if(!concurrentTransactionManager.isActiveTransaction())
                        throw new RuntimeException("Can't run another action. Transaction is not active");

                    return action.apply(previousResult);
                },
                concurrentTransactionManager.getCurrentExecutor(this));
        return this;
    }

    /**
     * Adds one new task to a new sequential chain running already in another thread from the new pool.
     * All transaction properties are transferred to the new thread.
     */
    @Override
    public TransactionAction putAnotherActionAsync(Function<? super Object, ?> action) {

        if(!concurrentTransactionManager.isActiveTransaction())
            throw new RuntimeException("Can't add another action. Transaction is not active");

        concurrentTransactionManager.addNewExecutor(this);

        completableFuture = completableFuture.thenApplyAsync(
                previousResult->{

                    if(!concurrentTransactionManager.isActiveTransaction())
                        throw new RuntimeException("Can't run another action. Transaction is not active");

                    setTransactionProperties();
                    return action.apply(previousResult);
                },
                concurrentTransactionManager.getCurrentExecutor(this));
        return this;
    }

    /**
     * Adds one new task to a new sequential chain running already in another thread from the new pool.
     * All transaction properties are transferred to the new thread.
     */
    @Override
    public TransactionAction putAnotherActionAsync(Function<? super Object, ?> action, ExecutorService executorService) {

        if(!concurrentTransactionManager.isActiveTransaction())
            throw new RuntimeException("Can't add another action. Transaction is not active");

        concurrentTransactionManager.addNewExecutor(this, executorService);

        completableFuture = completableFuture.thenApplyAsync(
                previousResult->{

                    if(!concurrentTransactionManager.isActiveTransaction())
                        throw new RuntimeException("Can't run another action. Transaction is not active");

                    setTransactionProperties();
                    return action.apply(previousResult);
                },
                concurrentTransactionManager.getCurrentExecutor(this));
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
        }
        return null;
    }

    /**
     * Transfers transaction properties to the child thread.
     */
    private void setTransactionProperties(){

        Map<Object, Object> resources = concurrentTransactionManager.getTransactionProperties().getResources();
        List<TransactionSynchronization> syncs = concurrentTransactionManager.getTransactionProperties().getSynchronizations();
        String txName = concurrentTransactionManager.getTransactionProperties().getCurrentTransactionName();
        Integer isoLevel = concurrentTransactionManager.getTransactionProperties().getCurrentTransactionIsolationLevel();
        Boolean active = concurrentTransactionManager.getTransactionProperties().getActualTransactionActive();

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