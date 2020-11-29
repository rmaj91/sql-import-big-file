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
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class DemoApplication {

	public static final int COLUMNS = 6_666_666;
	public static final int BATCH_SIZE = 2000;
	public static final ExecutorService executorService = Executors.newSingleThreadExecutor();

	public static void main(String[] args) throws DatabaseException, SQLException, InterruptedException {
		SpringApplication.run(DemoApplication.class, args);

		Connection connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter any sign to start...");
		scanner.nextLine();
		System.out.println("========================================");
		System.out.println("");
		System.out.println("========================================");

		log.info("Creating insert data");
		List<Map<String, String>> rowsData = new ArrayList<>(BATCH_SIZE);
		for (int i = 1; i <= COLUMNS; i++) {
			Map<String, String> map = new LinkedHashMap<>();
			map.put(new String("name1"), new String("name1"));
			map.put(new String("name2"), new String("name2"));
			map.put(new String("name3"), new String("name3"));
			map.put(new String("name4"), new String("name4"));
			map.put(new String("name5"), new String("name5"));
			rowsData.add(map);
			if (i % BATCH_SIZE == 0) {
				String insert = createInsert(rowsData);
				addImportsToQue(insert, connection);
				rowsData = new LinkedList<>();
				log.info("Batch nr: " + i/BATCH_SIZE + " created.");
			}
		}
		if (!rowsData.isEmpty()) {
			String insert = createInsert(rowsData);
			addImportsToQue(insert, connection);
		}
		log.info("Creating insert data finished.");

		executorService.shutdown();
		log.info("Waiting for all imports");

		executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		log.info("All imports finished.");

		connection.close();
		log.info("Db Connection closed.");
	}

	private static void addImportsToQue(String insert, Connection connection) {
		executorService.submit(new InsertDataJob(insert, connection));
	}

	private static String createInsert(List<Map<String, String>> objects) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append("KLASA ");
		sb.append("(");
		String columns = String.join(",", objects.get(0).keySet());
		sb.append(columns);
		sb.append(") values ");
		for (Map<String, String> object : objects) {
			sb.append("(");
			for (String value : object.values()) {
				if (value != null) {
					sb.append("'");
					sb.append(value);
					sb.append("'");
				}
				sb.append(",");
			}
			deleteLastChar(sb);
			sb.append("),");
		}
		deleteLastChar(sb);
		return sb.toString();
	}

	private static void deleteLastChar(StringBuilder sb) {
		sb.deleteCharAt(sb.length() - 1);
	}
}
