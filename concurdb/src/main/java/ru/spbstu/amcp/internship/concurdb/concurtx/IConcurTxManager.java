package ru.spbstu.amcp.internship.concurdb.concurtx;

import java.util.function.Supplier;

public interface IConcurTxManager {

    <T> T executeConcurTx(Supplier<T> action);
    Object createSavepoint();
    void rollbackToSavepoint(Object savePoint);
    void releaseSavepoint(Object savePoint);
    void setRollbackOnly();
    void setTxpolicy(int txpolicy);
    int getTxpolicy();

}
