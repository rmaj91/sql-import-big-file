package com.example.demo;


import liquibase.exception.DatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class DemoApplication {

	public static final int COLUMNS = 6_666_666;
	public static final int BATCH_SIZE = 25000;
	public static final ExecutorService executorService = Executors.newSingleThreadExecutor();

	public static void main(String[] args) throws DatabaseException, SQLException, InterruptedException {
		SpringApplication.run(DemoApplication.class, args);

		Connection connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter any sign to start...");
		scanner.nextLine();

		log.info("Creating insert data");
		LinkedList<Map<String, String>> rowsData = new LinkedList<>();
		for (int i = 0; i < COLUMNS; i++) {
			Map<String, String> map = new LinkedHashMap<>();
			map.put(new String("name1"), new String("name1"));
			map.put(new String("name2"), new String("name2"));
			map.put(new String("name3"), new String("name3"));
			map.put(new String("name4"), new String("name4"));
			map.put(new String("name5"), new String("name5"));
			rowsData.add(map);
			if (i % BATCH_SIZE == 0) {
				addImportsToQue(rowsData, connection);
				rowsData= new LinkedList<>();
			}
		}
		addImportsToQue(rowsData, connection);
		log.info("Creating insert data finished.");
		executorService.shutdown();
		log.info("Waiting for all imports");
		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		log.info("All imports finished.");
		connection.close();
		log.info("Db Connection closed.");
	}

	private static void addImportsToQue(LinkedList<Map<String, String>> rowsData, Connection connection) {
		executorService.submit(new InsertDataJob(rowsData, connection));
	}
}
