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
     * This property determines whether the transaction is active.
     */
    private AtomicBoolean isActiveTx = new AtomicBoolean(false);

    public boolean isActiveTransaction(){
        return isActiveTx.get();
    }

    /**
     * This property stores the transaction rollback policy number.
     */
    @Getter
    private TransactionRollbackPolicy txpolicy;

    /**
     * Данное свойство запускает транзакцию в императивном стиле.
     */
    @Getter
    private TransactionTemplate transactionTemplate;

    /**
     * Данное свойство содержит статус транзакции и необходимо для
     * реализации политик отката транзакции.
     */
    @Getter
    private TransactionStatus status;

    /**
     * Данное свойство хранит все запущенные цепочки задач - объекты TransactionAction
     */
    private Queue<TransactionAction> childTransactionActionQueue = new ConcurrentLinkedQueue<>();

    /**
     * Данное свойство содержит всю информацию о запущенной транзакции.
     * Свойство будет передаваться при асинхронном запуске цепочек задач
     * в thread local переменные новых потоков.
     */
    @Getter
    private List<Object> transactionProperties = new ArrayList<Object>();

    /**
     * Запомнить новую цепочку задач для ожидания завершения её выполнения
     * в методе commitLock перед коммитом транзакции.
     * @param child
     */
    void putChildTxAction(TransactionAction child){
        childTransactionActionQueue.add(child);
    }

    /**
     * Метод возвращает одну из цепочек задач, представленной объектом
     * TransactionAction для последующей блокировки потока, который запустил транзакци,
     * в методе commitLock, до тех пор, пока все задачи в объекте TransactionAction не выполнятся.
     * @return Одна из исполняемых цепочек задач
     */
    private TransactionAction getAnyChildTxAction(){
        return childTransactionActionQueue.poll();
    }

    /**
     * Метод запускает транзакцию и сохраняет всю информацию о транзакции,
     * а также её статус для реализации политик отката, как ручных, так и
     * автоматических.
     * @param action - выполняемая задача - внутри себя может содержать запуск
     *               последовательных и параллельных цепочек задач, формирующихся
     *               объектом класса TransactionAction
     * @param <T>
     * @return - результат выполнения тразакции
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
     * Откат тразакции на состояние до начала транзакции
     */
    public void setRollbackOnly(){
        status.setRollbackOnly();
    }


    /**
     * Метод блокирует поток, который запустил транзакцию перед
     * коммитом до тех пор, пока все цепочки задач не выполнятся.
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
     * Метод получает thread local переменные транзакции для переноса их в дочерние потоки
     */
    private void getTransactionPropertiesFromTransactionSynchronizationManager(){
        transactionProperties.add(TransactionSynchronizationManager.getResourceMap());
        transactionProperties.add(TransactionSynchronizationManager.getSynchronizations());
        transactionProperties.add(TransactionSynchronizationManager.getCurrentTransactionName());
        transactionProperties.add(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
        transactionProperties.add(TransactionSynchronizationManager.isActualTransactionActive());
    }

    /**
     * Конструктор создаёт менеджер параллельной транзакции
     * с существующим TransactionTemplate.
     * @param transactionTemplate объект для запуска транзакции
     */
    public ConcurrentTransactionManager(TransactionTemplate transactionTemplate){
        txpolicy = TransactionRollbackPolicy.DEFAULT_SPRING_JDBC_POLICY;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     *  Конструктор создаёт менеджер параллельной транзакции с существующим
     *  менеджером однопоточной транзакции.
     * @param transactionManager объект для формирования объекта класса TransactionTemplate,
     *                           который будет формировать транзакцию
     */
    public ConcurrentTransactionManager(PlatformTransactionManager transactionManager){
        txpolicy = TransactionRollbackPolicy.DEFAULT_SPRING_JDBC_POLICY;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Метод устанавливает политику отката транзакции
     * @param txpolicy политика отката
     */
    public void setTxpolicy(TransactionRollbackPolicy txpolicy){
        this.txpolicy = txpolicy;
    }

}
