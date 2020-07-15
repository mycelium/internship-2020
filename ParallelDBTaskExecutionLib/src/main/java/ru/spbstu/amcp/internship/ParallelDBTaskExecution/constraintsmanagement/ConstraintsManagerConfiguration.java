package ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Класс для автоконфигурирования приложения пользователя
 */
@Configuration
public class ConstraintsManagerConfiguration {

    @Bean
    @ConditionalOnClass(org.postgresql.Driver.class)
    public ConstraintsManager constraintsManager() {
        return new PostgresConstraintsManager();
    }

}
