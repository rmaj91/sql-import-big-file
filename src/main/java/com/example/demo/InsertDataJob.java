package com.example.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertDataJob implements Runnable {

    private final Connection connection;
    private final String insert;

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
        System.gc();
    }
}
