package ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx;

public interface ITxAction {

    TxAction thenRun();

    //Можно расширить до возврата полезных значений
    void get();

}
