package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services;

import org.springframework.stereotype.Service;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDao;


public interface UserService {

     void createUser(int id, String name);
}
