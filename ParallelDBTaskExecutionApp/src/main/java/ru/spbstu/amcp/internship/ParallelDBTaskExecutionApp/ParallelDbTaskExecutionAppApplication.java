package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.spbstu.amcp.internship.ParallelDBTaskExecution.service.MyService;

@SpringBootApplication(scanBasePackageClasses = {MyService.class})
@RestController
public class ParallelDbTaskExecutionAppApplication {

	private final MyService myService;

	public ParallelDbTaskExecutionAppApplication(MyService myService) {
		this.myService = myService;
	}

	@GetMapping("/")
	public String home() {
		return myService.message();
	}

	public static void main(String[] args) {
		SpringApplication.run(ParallelDbTaskExecutionAppApplication.class, args);
	}
}
