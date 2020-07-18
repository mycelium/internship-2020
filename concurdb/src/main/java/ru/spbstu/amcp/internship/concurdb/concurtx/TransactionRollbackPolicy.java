package ru.spbstu.amcp.internship.concurdb.concurtx;

public enum TransactionRollbackPolicy {

    DEFAULT_SPRING_JDBC_POLICY,
    ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD

}
