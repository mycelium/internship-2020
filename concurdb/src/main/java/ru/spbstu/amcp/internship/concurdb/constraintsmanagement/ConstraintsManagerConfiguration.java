package ru.spbstu.amcp.internship.concurdb.constraintsmanagement;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Класс для автоконфигурирования приложения пользователя
 */
@Configuration
public class ConstraintsManagerConfiguration {

    @Bean
    @ConditionalOnClass(name = "org.postgresql.Driver")
    public PostgresConstraintsManager postgresConstraintsManager() {
        return new PostgresConstraintsManager();
    }

    @Bean
    @ConditionalOnClass(name = "org.mariadb.jdbc.Driver")
    public MariaDBConstraintsManager mariaDBConstraintsManager() {
        return new MariaDBConstraintsManager();
    }

    @Bean
    @ConditionalOnClass(name = "org.sqlite.JDBC")
    public SQLiteConstraintsManager SQLiteConstraintsManager() {
        return new SQLiteConstraintsManager();
    }

}
