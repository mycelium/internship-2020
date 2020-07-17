package ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.services;

import lombok.Getter;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.concurdb.ConcurTxTesting.dao.UserDao;

@Service
public class UserServiceImpl implements UserService {

    private UserDao dao;
    @Getter
    private TransactionTemplate transactionTemplate;
    private TransactionTemplate transactionTemplate2;
    private PlatformTransactionManager mytransactionManager;

    public UserServiceImpl(UserDao dao, DataSourceTransactionManager transactionManager){
        this.dao = dao;
        mytransactionManager = transactionManager;
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate2 = new TransactionTemplate(transactionManager);
    }

}
