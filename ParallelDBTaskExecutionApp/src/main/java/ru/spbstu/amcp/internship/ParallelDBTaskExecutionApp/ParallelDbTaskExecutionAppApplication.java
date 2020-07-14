package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.ConstraintType;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.constraintsmanagement.PostgresConstraintsManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.dao.UserDaoImpl;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserServiceImpl;

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



		PostgresConstraintsManager pcm = new PostgresConstraintsManager(dao.getJdbcTemplate());
		List<Constraint> constraints = pcm.getAndInitAllConstraints("public", "car");
		List<Constraint> constraints2 = pcm.getAndInitAllConstraints("test_schema", "table_name");
		List<Constraint> constraints3 = pcm.getAndInitAllConstraints("public", "fruit");
		pcm.dropOneConstraint("test_schema", "table_name", "tata", ConstraintType.INDEX);
		pcm.restoreOneConstraint("test_schema", "table_name", "tata", ConstraintType.INDEX);

		pcm.dropOneConstraint("test_schema", "table_name", "pkk",  ConstraintType.PK);
		pcm.restoreOneConstraint("test_schema", "table_name", "pkk", ConstraintType.PK);

		pcm.dropOneConstraint("public", "car", "distfk", ConstraintType.FK);
		pcm.dropOneConstraint("public", "car", "unique_name", ConstraintType.UNIQUE);
		pcm.dropOneConstraint("public", "fruit", "some_checky", ConstraintType.CHECK);
		pcm.dropOneConstraint("public", "car", "car_idx", ConstraintType.INDEX);
		pcm.restoreOneConstraint("public", "car", "unique_name", ConstraintType.UNIQUE);
		pcm.restoreOneConstraint("public", "car", "distfk", ConstraintType.FK);
		pcm.restoreOneConstraint("public", "car", "car_idx", ConstraintType.INDEX);
		pcm.restoreOneConstraint("public", "fruit", "some_checky", ConstraintType.CHECK);


		Thread.sleep(100);



		System.out.println("DONE MAIN!");


	}

}
