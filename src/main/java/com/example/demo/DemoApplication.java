package com.example.demo;


import liquibase.exception.DatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws DatabaseException, SQLException, InterruptedException {
		SpringApplication.run(DemoApplication.class, args);

		Connection connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter any sign to start...");
		scanner.nextLine();
		System.out.println("#========================================");
		System.out.println("");
		System.out.println("#========================================");

		DataService dataService = new DataService(connection);
		dataService.importData();

		connection.close();
		log.info("Db Connection closed.");
	}


}
