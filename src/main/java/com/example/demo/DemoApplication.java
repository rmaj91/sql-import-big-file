package com.example.demo;


import liquibase.exception.DatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
@Slf4j
@SpringBootApplication
public class DemoApplication {

	public static final int COLUMNS = 666_666;

	public static void main(String[] args) throws DatabaseException, SQLException {
		SpringApplication.run(DemoApplication.class, args);

		Connection connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");

		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();

		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();

		log.info("1");
		LinkedList<Map<String, String>> objects = new LinkedList<>();
		for (int i = 0; i < COLUMNS; i++) {
			Map<String, String> map = new LinkedHashMap<>();
			map.put(new String("name1"), new String("name1"));
			map.put(new String("name2"), new String("name2"));
			map.put(new String("name3"), new String("name3"));
			map.put(new String("name4"), new String("name4"));
			map.put(new String("name5"), new String("name5"));
			objects.add(map);
		}
		log.info("2");

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
			sb.deleteCharAt(sb.length() - 1);
			sb.append("),");
		}
		sb.deleteCharAt(sb.length() - 1);
//		System.out.println(sb.toString());

		log.info("3");
		statement.execute(sb.toString());
		connection.commit();
		log.info("4");

		connection.close();

	}

}
