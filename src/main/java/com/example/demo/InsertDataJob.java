package com.example.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.example.demo.DataService.MAX_QUEUE_SIZE;

public class InsertDataJob implements Runnable {

    private static int nr = 0;

    private final Connection connection;
    private String insert;

    public InsertDataJob(String insert, Connection connection) {
        this.connection = connection;
        this.insert = insert;
    }

    @Override
    public void run() {
        try (Statement statement = connection.createStatement()){
            statement.execute(insert);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println("Batch nr: " + ++nr +" imported");
        if (nr % MAX_QUEUE_SIZE == 0) {
            System.gc();
        }
    }
}
