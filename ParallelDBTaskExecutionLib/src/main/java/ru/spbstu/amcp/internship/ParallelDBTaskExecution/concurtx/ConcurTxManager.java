package ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx;

import lombok.Getter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;


public class ConcurTxManager {

    //Для создания транзакции
    private TransactionTemplate transactionTemplate;

    //Для возможности осуществить rollback и создать savepoint в задачах
    @Getter
    private TransactionStatus status;

    //Родительский поток не делает коммит, пока есть исполняемый дочерний поток в транзакции
    private Queue<TxAction> childTxActionQueue = new ConcurrentLinkedQueue<>();

    //Запомнить новый дочерний поток в транзакции
    public void putChildTxAction(TxAction child){
        childTxActionQueue.add(child);
    }

    //Получить один из дочерних потоков в транзакции
    public TxAction getAnyChildTxAction(){
        return childTxActionQueue.poll();
    }

    //Запустить транзакцию и получить статус
    public <T> T executeConcurTx(Supplier<T> action){
        return transactionTemplate.execute(s -> {
            status = s;
            T result = action.get();

            //Блокируем исполнение потока,
            //если есть хотя бы один дочерний поток
            commitLock();

            return result;
        });
    }


    private void commitLock(){
        while(!childTxActionQueue.isEmpty()){
            getAnyChildTxAction().get();
        }
    }

    //Создать менеджер параллельной транзакции с существующим TransactionTemplate
    public ConcurTxManager(TransactionTemplate transactionTemplate){
        this.transactionTemplate = transactionTemplate;
    }

    //Создать менеджер параллельной транзакции с существующим менеджером однопоточной транзакции
    public ConcurTxManager(PlatformTransactionManager transactionManager){
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

}
