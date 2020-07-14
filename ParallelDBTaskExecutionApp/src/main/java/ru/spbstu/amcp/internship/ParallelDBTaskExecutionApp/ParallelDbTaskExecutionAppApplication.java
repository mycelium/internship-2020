package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.ConcurTxManager;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.concurtx.TxAction;
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
//		pcm.dropOneConstraint("test_schema", "table_name", "tata", ConstraintType.INDEX);
//		pcm.restoreOneConstraint("test_schema", "table_name", "tata", ConstraintType.INDEX);
//
//		pcm.dropOneConstraint("test_schema", "table_name", "pkk",  ConstraintType.PK);
//		pcm.dropOneConstraint("test_schema", "table_name", "pkk",  ConstraintType.PK);
//		pcm.restoreOneConstraint("test_schema", "table_name", "pkk", ConstraintType.PK);
//
//		pcm.dropOneConstraint("public", "car", "prettyfk", ConstraintType.FK);
//		pcm.dropOneConstraint("public", "car", "unique_name", ConstraintType.UNIQUE);
//		pcm.dropOneConstraint("public", "fruit", "some_checky", ConstraintType.CHECK);
//		pcm.dropOneConstraint("public", "car", "car_idx", ConstraintType.INDEX);
//		pcm.restoreOneConstraint("public", "car", "unique_name", ConstraintType.UNIQUE);
//		pcm.restoreOneConstraint("public", "car", "prettyfk", ConstraintType.FK);
//		pcm.restoreOneConstraint("public", "car", "car_idx", ConstraintType.INDEX);
//		pcm.restoreOneConstraint("public", "fruit", "some_checky", ConstraintType.CHECK);
//
//		pcm.dropOneConstraint("public", "car", "name", ConstraintType.DEFAULT);
//		pcm.restoreOneConstraint("public", "car", "name", ConstraintType.DEFAULT);


		List<Constraint> cons = pcm.getAndInitAllConstraints("test_schema", "car");
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

		for (int i = 0; i < 100; i++){
			dao.getJdbcTemplate().execute("insert into test_schema.car (id, name, user_name, user_id," +
					"value, autointf) VALUES ("+i+", 'Test', 'name', 1, 30, 100)");
		}

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


		pcm.getAndInitAllConstraints("test_schema", "car2");
		pcm.dropAllConstraintsInTable("test_schema", "car2", true,
				ConstraintType.CHECK, ConstraintType.DEFAULT, ConstraintType.FK, ConstraintType.PK,
				ConstraintType.INDEX, ConstraintType.NOT_NULL, ConstraintType.UNIQUE);


		pcm.restoreAllConstraintsInTable("test_schema", "car2", true);


		Thread.sleep(100);



		System.out.println("DONE MAIN!");


	}

}
