package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.PostgresConstraintsManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDaoImpl;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserService;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserServiceImpl;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@RestController
public class ParallelDbTaskExecutionAppApplication {



	public static void main(String[] args) throws Exception {

		ApplicationContext context = SpringApplication.run(ParallelDbTaskExecutionAppApplication.class, args);
		UserServiceImpl userService = context.getBean(UserServiceImpl.class);

		userService.myTx();

		UserDaoImpl dao = context.getBean(UserDaoImpl.class);

//		new PostgresConstraintsManager(dao.getJdbcTemplate())
//				.dropConstraint("car", "postgresfk");

//		new PostgresConstraintsManager(dao.getJdbcTemplate())
//				.addPrimaryKey("users", "PostgresPK",
//						Arrays.asList("id", "name"));
//
//		new PostgresConstraintsManager(dao.getJdbcTemplate())
//				.addForeignKey("car", "PostgresFK",
//						 Arrays.asList("user_id", "user_name"),
//						"users2", Arrays.asList("id", "name"));


		System.out.println("---------------------------------");
		System.out.println(new PostgresConstraintsManager
				(dao.getJdbcTemplate()).getSchemaPrivileges("test_schema"));
		System.out.println(new PostgresConstraintsManager
				(dao.getJdbcTemplate()).getTableOwner("test_schema", "table_name"));



		String ddl = new PostgresConstraintsManager(dao.getJdbcTemplate())
				.getTableDDL("test_schema", "table_name");

		Thread.sleep(100);
		System.out.println(ddl);


		System.out.println("DONE MAIN!");


	}

}
