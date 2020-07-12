package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.model;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@AllArgsConstructor
public class User {
    private int id;
    private String name;
}
