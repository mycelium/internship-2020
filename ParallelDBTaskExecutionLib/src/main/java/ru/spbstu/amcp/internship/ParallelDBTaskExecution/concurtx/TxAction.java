package ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

/***
 * Идея: Делегировать работу цепочки CompletableFuture
 * Проблема: thenRun у CompletableFuture может выполнятся
 * в разных потоках
 * Решение: Использовать newSingleThreadExecutor и метод
 * thenRunAsync с передачей executorService для контроля
 * потока
 */
public class TxAction implements ITxAction {

    ConcurTxManager concurTxManager;
    List<ExecutorService> executorServices = new ArrayList<>();
    CompletableFuture<?> completableFuture = null;

    public TxAction(ConcurTxManager concurTxManager) {
        this.concurTxManager = concurTxManager;
        executorServices.add(Executors.newSingleThreadExecutor());
        concurTxManager.putChildTxAction(this);
    }

    //Стартуем первую задачу
    @Override
    public TxAction startAction(Supplier<? extends Object> action) {
        completableFuture = CompletableFuture.supplyAsync(()->{
            setTransactionProperties();
            return action.get();
        }, executorServices.get(0));
        return this;
    }

    //Добавляем ещу одну задачу в очередь на выполнение в рамках одного потока
    @Override
    public TxAction putAnotherAction(Function<? super Object, ?> action) {
        //Транзакция останется действительной
        completableFuture = completableFuture.thenApplyAsync(action,
                executorServices.get(executorServices.size()-1));
        return this;
    }

    //Пока черновой вариант
    @Override
    public Object get() {
        try {
            //Получаем результат последнего действия из цепочки
            Object result = completableFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }finally {
            for (var executor: executorServices) {
                executor.shutdown();
            }
        }
        return null;
    }

    //Устанавливаю свойства текущей транзакции в дочернем потоке
    private void setTransactionProperties(){
        List<Object> props = new ArrayList<>();
        props.addAll(concurTxManager.getTransactionProperties());

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
