package ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ITxAction {

    TxAction startAction(Supplier<? extends Object> action);
    TxAction putAnotherAction(Function<? super Object, ?> action);
    TxAction putAnotherActionAsync(Function<? super Object, ?> action);
    Object get() throws ExecutionException;

}
