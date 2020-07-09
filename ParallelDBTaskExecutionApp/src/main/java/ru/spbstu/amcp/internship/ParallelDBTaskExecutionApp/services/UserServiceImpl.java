package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services;

import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDao;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.model.User;

@Service
public class UserServiceImpl implements UserService {

    private UserDao dao;
    private TransactionTemplate transactionTemplate;

    public UserServiceImpl(UserDao dao, PlatformTransactionManager transactionManager){
        this.dao = dao;
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public void createUser(int id, String name){
        dao.insert(new User(id, name));
    }

    @Transactional(rollbackFor = Exception.class)
    public void userTestNonParallelDeclarativeTransaction() throws Exception {

            dao.insert(new User(2, "Test"));
            dao.insert(new User(5,"Petr"));
            dao.changeUserName(5, "Ivan");
            dao.insert(new User(6,"John"));
            dao.deleteById(2);
            throw new Exception("roll back");

    }

    public void userTestNonParallelImperativeTransaction() throws Exception {

        System.out.println(transactionTemplate);
        transactionTemplate.execute(status -> {
            try {

                dao.insert(new User(7, "Test"));
                dao.insert(new User(8, "Petr"));
                dao.changeUserName(9, "Ivan");
                dao.insert(new User(10, "John"));
                dao.deleteById(2);
                //status.setRollbackOnly();
            }
            catch (Exception ex){
                status.setRollbackOnly();
            }

            return null;
        });
    }


}
