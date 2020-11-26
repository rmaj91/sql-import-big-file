package com.example.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;

public class InsertDataJob implements Runnable {

    private Connection connection;
    private LinkedList<Map<String, String>> rowsData;

    public InsertDataJob(LinkedList<Map<String, String>> rowsData, Connection connection) {
        this.connection = connection;
        this.rowsData = rowsData;
    }

    @Override
    public void run() {
        String insert = createInsert(rowsData);
        try (Statement statement = connection.createStatement()){
            statement.execute(insert);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static String createInsert(LinkedList<Map<String, String>> objects) {
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
