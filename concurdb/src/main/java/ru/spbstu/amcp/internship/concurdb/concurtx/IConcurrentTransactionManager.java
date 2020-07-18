package ru.spbstu.amcp.internship.concurdb.concurtx;

import java.util.function.Supplier;

public interface IConcurrentTransactionManager {

    <T> T executeConcurrentTransaction(Supplier<T> action);
    Object createSavepoint();
    void rollbackToSavepoint(Object savePoint);
    void releaseSavepoint(Object savePoint);
    void setRollbackOnly();
    void setTxpolicy(TransactionRollbackPolicy txpolicy);
    TransactionRollbackPolicy getTxpolicy();

}
