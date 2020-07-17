package ru.spbstu.amcp.internship.concurdb.concurtx;

public interface TransactionRollbackPolicy {

    int DEFAULT_SPRING_JDBC_POLICY = 0;
    int ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD = 1;

}
