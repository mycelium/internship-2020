package ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.services;

import lombok.Getter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.dao.UserDao;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.ConcurTxTesting.model.User;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TransactionRollbackPolicy;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TxAction;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.extra.PDataSourceTransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class UserServiceImpl implements UserService {

    private UserDao dao;
    @Getter
    private TransactionTemplate transactionTemplate;
    private TransactionTemplate transactionTemplate2;
    private PlatformTransactionManager mytransactionManager;

    public UserServiceImpl(UserDao dao, PDataSourceTransactionManager transactionManager){
        this.dao = dao;
        mytransactionManager = transactionManager;
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate2 = new TransactionTemplate(transactionManager);
    }

}
