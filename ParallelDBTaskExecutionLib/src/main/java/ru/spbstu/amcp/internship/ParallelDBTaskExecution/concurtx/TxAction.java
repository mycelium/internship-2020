package ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx;

/***
 * Цепочка последовательных задач - по сути урезанный CompletableFuture
 */
public class TxAction implements ITxAction {

    ConcurTxManager concurTxManager;

    public TxAction(ConcurTxManager concurTxManager) {
        this.concurTxManager = concurTxManager;
        concurTxManager.putChildTxAction(this);
    }

    @Override
    public TxAction thenRun() {
        return null;
    }

    @Override
    public void get() {

    }
}
