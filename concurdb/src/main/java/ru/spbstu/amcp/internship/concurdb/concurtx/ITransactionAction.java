package ru.spbstu.amcp.internship.concurdb.concurtx;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ITransactionAction {

    TransactionAction startAction(Supplier<? extends Object> action);
    TransactionAction putAnotherAction(Function<? super Object, ?> action);
    TransactionAction putAnotherActionAsync(Function<? super Object, ?> action);
    Object get() throws ExecutionException;

}
