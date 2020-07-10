package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.JdbcProperties;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.support.PDataSourceTransactionManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.support.PJdbcTemplate;

import javax.sql.DataSource;

@Configuration
@PropertySource("classpath:application.yml")
public class DBConfig {

    @Bean
    public DataSource dataSource(){
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl("jdbc:postgresql://127.0.0.1:5432/TestDB");
        ds.setUsername("postgres");
        ds.setPassword("root");
        return ds;
    }

    @Bean
    @ConditionalOnMissingBean({PlatformTransactionManager.class})
    PDataSourceTransactionManager transactionManager(DataSource dataSource, ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
        PDataSourceTransactionManager transactionManager = new PDataSourceTransactionManager(dataSource);
        System.out.println("AAAAA" + dataSource);
        transactionManagerCustomizers.ifAvailable((customizers) -> {
            customizers.customize(transactionManager);
        });
        return transactionManager;
    }

    @Bean
    @Primary
    PJdbcTemplate jdbcTemplate(DataSource dataSource, JdbcProperties properties) {
        PJdbcTemplate jdbcTemplate = new PJdbcTemplate(dataSource);
        JdbcProperties.Template template = properties.getTemplate();
        jdbcTemplate.setFetchSize(template.getFetchSize());
        jdbcTemplate.setMaxRows(template.getMaxRows());
        if (template.getQueryTimeout() != null) {
            jdbcTemplate.setQueryTimeout((int)template.getQueryTimeout().getSeconds());
        }

        return jdbcTemplate;
    }

}
