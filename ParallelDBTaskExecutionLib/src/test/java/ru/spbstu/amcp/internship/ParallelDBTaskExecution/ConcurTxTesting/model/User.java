package ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.model;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class User {
    private int id;
    private String name;
}
