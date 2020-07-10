package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.model.User;

import java.util.List;

@Repository
public class UserDaoImpl implements UserDao {

    private JdbcTemplate jdbcTemplate;

    public UserDaoImpl(JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate = jdbcTemplate;
        jdbcTemplate.update("DROP TABLE IF EXISTS USERS");
        jdbcTemplate.update("CREATE TABLE USERS(ID INT PRIMARY KEY, NAME VARCHAR(255))");

    }

    @Override
    public void insert(User user) {
        jdbcTemplate.update("INSERT INTO USERS (id, name) values (?, ?)", user.getId(), user.getName());
    }


    public void deleteById(int id) {
        jdbcTemplate.update("DELETE FROM USERS WHERE ID = ?", id);
    }

    public int count() {
        return 0;
    }

    public User getById(int id) {
        return jdbcTemplate.queryForObject("SELECT * FROM USERS WHERE ID = ?", new Object[]{id},
                (rs,i) -> new User(rs.getInt("id"), rs.getString("name")));
    }


    public List<User> getAll() {
        return null;
    }


    public void changeUserName(int id, String newName) {
        jdbcTemplate.update("UPDATE USERS SET name=? where id = ?", newName, id);
    }

}
