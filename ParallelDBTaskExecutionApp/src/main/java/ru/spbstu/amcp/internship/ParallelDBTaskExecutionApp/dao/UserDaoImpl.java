package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.model.User;

import java.time.Period;
import java.util.List;

@Repository
public class UserDaoImpl implements UserDao {

    private JdbcOperations jdbcOperations;

    public UserDaoImpl(JdbcOperations jdbcOperations)
    {
        this.jdbcOperations = jdbcOperations;
        jdbcOperations.update("DROP TABLE IF EXISTS USERS");
        jdbcOperations.update("CREATE TABLE USERS(ID INT PRIMARY KEY, NAME VARCHAR(255))");

    }

    @Override
    public void insert(User user) {
        jdbcOperations.update("INSERT INTO USERS (id, name) values (?, ?)", user.getId(), user.getName());
    }


    public void deleteById(int id) {
        jdbcOperations.update("DELETE FROM USERS WHERE ID = ?", id);
    }

    public int count() {
        return 0;
    }

    public User getById(int id) {
        return jdbcOperations.queryForObject("SELECT * FROM USERS WHERE ID = ?", new Object[]{id},
                (rs,i) -> new User(rs.getInt("id"), rs.getString("name")));
    }


    public List<User> getAll() {
        return null;
    }


    public void changeUserName(int id, String newName) {
        jdbcOperations.update("UPDATE USERS SET name=? where id = ?", newName, id);
    }

}
