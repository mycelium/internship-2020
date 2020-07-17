package ru.spbstu.amcp.internship.concurdbapp.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;

@Configuration
@PropertySource("classpath:application.yml")
public class DBConfig {

//    @Bean
//    public DataSource dataSource(){
//        DriverManagerDataSource ds = new DriverManagerDataSource();
//        ds.setDriverClassName("org.postgresql.Driver");
//        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/TestDB");
//        ds.setUsername("postgres");
//        ds.setPassword("root");
//        return ds;
//    }

    @Bean
    public DataSource dataSource(){
        SQLiteDataSource ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:ParallelDBTaskExecutionLib\\src\\main\\resources\\sql.db");
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        return ds;
    }

//
//    @Bean
//    public DataSource dataSource(){
//        DriverManagerDataSource ds = new DriverManagerDataSource();
//        ds.setDriverClassName("org.mariadb.jdbc.Driver");
//        ds.setUrl("jdbc:mariadb://localhost:3307/test_schema");
//        ds.setUsername("root");
//        ds.setPassword("root");
//        return ds;
//    }

    @Bean
    @ConditionalOnMissingBean({PlatformTransactionManager.class})
    DataSourceTransactionManager transactionManager(DataSource dataSource, ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        System.out.println(dataSource);
        transactionManagerCustomizers.ifAvailable((customizers) -> {
            customizers.customize(transactionManager);
        });
        return transactionManager;
    }


}
