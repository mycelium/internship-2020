package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TxAction;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.*;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDaoImpl;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserServiceImpl;

import java.util.List;

@SpringBootApplication
@RestController
/**
 * Чтобы заработал autowire у ConstraintsManager
 */
@Import(ConstraintsManagerConfiguration.class)
public class ParallelDbTaskExecutionAppApplication {


	public static void testConstraintsManager(ApplicationContext context, JdbcTemplate jdbcTemplate){

		//Сначала дропнем таблицы и заново их инициализируем
		Resource resource = new ClassPathResource("car.sql");
		ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
		databasePopulator.execute(jdbcTemplate.getDataSource());


		//Spring сам определит по драйверу какой ConstraintsManager ему нужен (для Postgres, MariaDB или SqLite)
		ConstraintsManager pcm = context.getBean(ConstraintsManager.class);
		//Выведет тип драйвера
		System.out.println(pcm.driverType());

		//ConstraintsManager запомнит все constraints для указанной таблицы в схеме
		//Это делать обязательно перед отключением constraints
		List<Constraint> cons = pcm.getAndInitAllConstraints("test_schema", "car");

		//Отключаем constraints
		pcm.dropOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE);
		pcm.dropOneConstraint("test_schema", "car", "car_pkey", ConstraintType.PK);
		pcm.dropOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT);
		pcm.dropOneConstraint("test_schema", "car", "autointf", ConstraintType.NOT_NULL);
		pcm.dropOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT);
		pcm.dropOneConstraint("test_schema", "car", "name", ConstraintType.NOT_NULL);
		pcm.dropOneConstraint("test_schema", "car", "distfk", ConstraintType.FK);
		pcm.dropOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK);
		pcm.dropOneConstraint("test_schema", "car", "car_idx", ConstraintType.INDEX);
		pcm.dropOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX);

		//Здесь вставятся данные, не удовлетворяющие constraints
		for (int i = 0; i < 100; i++){
			jdbcTemplate.execute("insert into test_schema.car (id, name, user_name, user_id," +
					"value, autointf) VALUES ("+i+", 'Test', 'name', 1, 30, 100)");
		}

		//Включаем constraints - некоторые constraints не включатся, так как нарушены условия после вставки, но они (constraints) не удалятся,
		//пока работает программа
		pcm.restoreOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX,true);
		pcm.restoreOneConstraint("test_schema", "car", "car_idx", ConstraintType.INDEX,true);
		pcm.restoreOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE,true);
		pcm.restoreOneConstraint("test_schema", "car", "car_pkey", ConstraintType.PK,true);
		pcm.restoreOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT,true);
		pcm.restoreOneConstraint("test_schema", "car", "autointf", ConstraintType.NOT_NULL,true);
		pcm.restoreOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT,true);
		pcm.restoreOneConstraint("test_schema", "car", "name", ConstraintType.NOT_NULL,true);
		pcm.restoreOneConstraint("test_schema", "car", "distfk", ConstraintType.FK,true);
		pcm.restoreOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK,true);

		//Другой вариант удаления/восстановления constraints

		databasePopulator.execute(jdbcTemplate.getDataSource());

		pcm.getAndInitAllConstraints("test_schema", "car");

		pcm.dropAllConstraintsInTable("test_schema", "car", true,
				ConstraintType.CHECK, ConstraintType.DEFAULT, ConstraintType.FK, ConstraintType.PK,
				ConstraintType.INDEX, ConstraintType.NOT_NULL, ConstraintType.UNIQUE);


		pcm.restoreAllConstraintsInTable("test_schema", "car", true);

	}


	public static void main(String[] args) throws Exception {

		ApplicationContext context = SpringApplication.run(ParallelDbTaskExecutionAppApplication.class, args);
		UserServiceImpl userService = context.getBean(UserServiceImpl.class);

		userService.myTx();
		UserDaoImpl dao = context.getBean(UserDaoImpl.class);

		testConstraintsManager(context, dao.getJdbcTemplate());

		Thread.sleep(100);

		System.out.println("DONE MAIN!");


	}

}
