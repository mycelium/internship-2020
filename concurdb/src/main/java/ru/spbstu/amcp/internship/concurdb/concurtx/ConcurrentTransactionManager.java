package ru.spbstu.amcp.internship.concurdb.concurtx;

import lombok.Getter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A class whose instances form transactions,
 * inside which chains of asynchronous and sequential tasks can be executed.
 */
@Service
public class ConcurrentTransactionManager implements IConcurrentTransactionManager {

    /**
     * Determines whether the transaction is active.
     */
    private AtomicBoolean isActiveTx = new AtomicBoolean(false);

    public boolean isActiveTransaction(){
        return isActiveTx.get();
    }

    /**
     * Stores the transaction rollback policy.
     */
    @Getter
    private TransactionRollbackPolicy txpolicy;

    /**
     * Starts the transaction in an imperative style.
     */
    @Getter
    private TransactionTemplate transactionTemplate;

    /**
     * Contains transaction status and is necessary for
     * implementing transaction rollback policies.
     */
    @Getter
    private TransactionStatus status;

    /**
     * Stores all running task chains (TransactionAction instances).
     */
    private Queue<TransactionAction> childTransactionActionQueue = new ConcurrentLinkedQueue<>();

    /**
     * Contains all information about a running transaction.
     * The property will be passed to thread local variables of new threads.
     */
    @Getter
    private List<Object> transactionProperties = new ArrayList<Object>();

    /**
     * Remembers a new task chain to wait for its completion
     * in the commitLock method before committing the transaction.
     * @param child
     */
    void putChildTxAction(TransactionAction child){
        childTransactionActionQueue.add(child);
    }

    /**
     * Returns one of the task chains represented by the instance of
     * TransactionAction to subsequently lock the thread that started the transaction
     * in the commitLock method until all tasks in the TransactionAction instance are completed.
     */
    private TransactionAction getAnyChildTxAction(){
        return childTransactionActionQueue.poll();
    }

    /**
     * Starts the transaction and saves all the information about the transaction,
     * as well as its status for the implementation of rollback policies, both manual and
     * automatic.
     * @param action - may contain sequential and parallel
     *               chains of tasks formed by an instance of the class TransactionAction
     * @param <T>
     * @return - result of the transaction
     */
    public <T> T executeConcurrentTransaction(Supplier<T> action){
        if(isActiveTx.get())
            throw new CannotCreateTransactionException("Transaction is already active (use another instance of ConcurrentTransactionManager)");

        isActiveTx.set(true);
        return transactionTemplate.execute(s -> {
            status = s;
            getTransactionPropertiesFromTransactionSynchronizationManager();

            T result = action.get();

            //Блокируем исполнение потока,
            //если есть хотя бы один дочерний поток
            commitLock();
            isActiveTx.set(false);
            return result;
        });
    }

    /**
     * Imported from Spring:
     * Create a new savepoint. You can roll back to a specific savepoint
     * via {@code rollbackToSavepoint}, and explicitly release a savepoint
     * that you don't need anymore via {@code releaseSavepoint}.
     * <p>Note that most transaction managers will automatically release
     * savepoints at transaction completion.
     * @return a savepoint object, to be passed into
     * {@link #rollbackToSavepoint} or {@link #releaseSavepoint}
     * @throws NestedTransactionNotSupportedException if the underlying
     * transaction does not support savepoints
     * @throws TransactionException if the savepoint could not be created,
     * for example because the transaction is not in an appropriate state
     * @see java.sql.Connection#setSavepoint
     */
    public Object createSavepoint(){
        return status.createSavepoint();
    }


    /**
     * Imported from Spring:
     * Roll back to the given savepoint.
     * <p>The savepoint will <i>not</i> be automatically released afterwards.
     * You may explicitly call {@link #releaseSavepoint(Object)} or rely on
     * automatic release on transaction completion.
     * @param savePoint the savepoint to roll back to
     * @throws NestedTransactionNotSupportedException if the underlying
     * transaction does not support savepoints
     * @throws TransactionException if the rollback failed
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollbackToSavepoint(Object savePoint){
        status.rollbackToSavepoint(savePoint);
    }

    /**
     * Imported from Spring:
     * Explicitly release the given savepoint.
     * <p>Note that most transaction managers will automatically release
     * savepoints on transaction completion.
     * <p>Implementations should fail as silently as possible if proper
     * resource cleanup will eventually happen at transaction completion.
     * @param savePoint the savepoint to release
     * @throws NestedTransactionNotSupportedException if the underlying
     * transaction does not support savepoints
     * @throws TransactionException if the release failed
     * @see java.sql.Connection#releaseSavepoint
     */
    public void releaseSavepoint(Object savePoint){
        status.releaseSavepoint(savePoint);
    }

    /**
     * Imported from Spring:
     * Set the transaction rollback-only. This instructs the transaction manager
     * that the only possible outcome of the transaction may be a rollback, as
     * alternative to throwing an exception which would in turn trigger a rollback.
     *
     * Rollback transaction to the state before transaction
     */
    public void setRollbackOnly(){
        status.setRollbackOnly();
    }


    /**
     * Blocks the thread that started the transaction before
     * commit until all task chains are completed.
     */
    private void commitLock(){
        while(!childTransactionActionQueue.isEmpty()){
            try {
                getAnyChildTxAction().get();
            }catch (ExecutionException exception){
                System.out.println("EXCEPTION IN CHILD THREAD!");
                if(txpolicy ==
                        TransactionRollbackPolicy.ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD)
                    this.setRollbackOnly();
            }
        }
    }

    /**
     * Gets thread local transaction variables for transferring them to child threads
     */
    private void getTransactionPropertiesFromTransactionSynchronizationManager(){
        transactionProperties.add(TransactionSynchronizationManager.getResourceMap());
        transactionProperties.add(TransactionSynchronizationManager.getSynchronizations());
        transactionProperties.add(TransactionSynchronizationManager.getCurrentTransactionName());
        transactionProperties.add(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
        transactionProperties.add(TransactionSynchronizationManager.isActualTransactionActive());
    }

    /**
     * Creates a parallel transaction manager
     * with an existing instance of TransactionTemplate.
     */
    public ConcurrentTransactionManager(TransactionTemplate transactionTemplate){
        txpolicy = TransactionRollbackPolicy.DEFAULT_SPRING_JDBC_POLICY;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     *  Creates a parallel transaction manager with an existing Spring
     *  single-threaded transaction manager.
     */
    public ConcurrentTransactionManager(PlatformTransactionManager transactionManager){
        txpolicy = TransactionRollbackPolicy.DEFAULT_SPRING_JDBC_POLICY;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Sets transaction rollback policy
     */
    public void setTxpolicy(TransactionRollbackPolicy txpolicy){
        this.txpolicy = txpolicy;
    }

}
