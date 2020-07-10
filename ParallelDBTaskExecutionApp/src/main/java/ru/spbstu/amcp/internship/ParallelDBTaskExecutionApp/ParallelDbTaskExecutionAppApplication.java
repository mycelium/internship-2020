package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserService;
import ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp.services.UserServiceImpl;

@SpringBootApplication
@RestController
public class ParallelDbTaskExecutionAppApplication {

	public static void main(String[] args) throws Exception {

		ApplicationContext context = SpringApplication.run(ParallelDbTaskExecutionAppApplication.class, args);
		UserServiceImpl userService = context.getBean(UserServiceImpl.class);
		userService.createUser(3, "User");
		try {
			userService.userTestNonParallelDeclarativeTransaction();
		}catch (Exception rollback){}
		userService.userTestParallelImperativeTransaction();

//		for(int i = 0; i < 100000; i++){
//			userService.createUser(i+15, "Test");
//		}

	}

}
