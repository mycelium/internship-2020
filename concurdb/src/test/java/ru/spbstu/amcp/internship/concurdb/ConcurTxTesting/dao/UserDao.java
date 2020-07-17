package ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.dao;


import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.model.User;

import java.util.List;

public interface UserDao {

    void insert(User user);
    void deleteById(int id);
    int count();
    User getById(int id);
    List<User> getAll();
    void changeUserName(int id, String newName);

}
