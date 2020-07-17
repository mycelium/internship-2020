package ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.dao;

import lombok.Getter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.model.User;

import java.util.List;

@Repository
public class UserDaoImpl implements UserDao {

    @Getter
    private JdbcTemplate jdbcTemplate;

    public UserDaoImpl(JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate = jdbcTemplate;
        jdbcTemplate.update("DROP TABLE IF EXISTS USERS5");
        jdbcTemplate.update("CREATE TABLE USERS5(ID INT PRIMARY KEY, NAME VARCHAR(255))");

    }

    @Override
    public void insert(User user) {
        jdbcTemplate.update("INSERT INTO USERS5 (id, name) values (?, ?)", user.getId(), user.getName());
    }


    public void deleteById(int id) {
        jdbcTemplate.update("DELETE FROM USERS5 WHERE ID = ?", id);
    }

    public int count() {
        return 0;
    }

    public User getById(int id) {
        return jdbcTemplate.queryForObject("SELECT * FROM USERS5 WHERE ID = ?", new Object[]{id},
                (rs,i) -> new User(rs.getInt("id"), rs.getString("name")));
    }


    public List<User> getAll() {
        return null;
    }


    public void changeUserName(int id, String newName) {
        jdbcTemplate.update("UPDATE USERS5 SET name=? where id = ?", newName, id);
    }

}
