package com.example.demo;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DataService {

    public static final int ROWS = 6_666_666;
    public static final int BATCH_SIZE = 50_000;
    public static final int MAX_QUEUE_SIZE = 10;
    public static final int MIN_QUEUE_SIZE = 5;

    private final Connection connection;
    private final ThreadPoolExecutor executor;

    public DataService(Connection connection) {
        this.connection = connection;
        this.executor =  (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    }

    @SneakyThrows
    public void importData() {
        log.info("Creating insert data");
        List<Map<String, String>> rowsData = new ArrayList<>(BATCH_SIZE);
        for (int i = 1; i <= ROWS; i++) {
            Map<String, String> map = new LinkedHashMap<>(5);
            map.put(new String("name1"), new String("name1name1name1name1name1"));
            map.put(new String("name2"), new String("name2name2name2name2name2"));
            map.put(new String("name3"), new String("name3name3name3name3name3"));
            map.put(new String("name4"), new String("name4name4name4name4name4"));
            map.put(new String("name5"), new String("name5name5name5name5name5"));
            rowsData.add(map);
            if (i == BATCH_SIZE) {
                if (executor.getQueue().size() > MAX_QUEUE_SIZE) {
                    while (executor.getQueue().size() > MIN_QUEUE_SIZE) {
                        Thread.sleep(200);
                    }
                }
                String insert = createInsert(rowsData);
                addImportsToQue(insert, connection);
                rowsData = new ArrayList<>(BATCH_SIZE);
                log.info("Batch nr: " + i/BATCH_SIZE + " created.");
            }
        }
        if (!rowsData.isEmpty()) {
            String insert = createInsert(rowsData);
            addImportsToQue(insert, connection);
        }
        log.info("Creating insert data finished.");
        executor.shutdown();
        log.info("Waiting for all imports");
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        log.info("All imports finished.");
        System.gc();
    }

    private void addImportsToQue(String insert, Connection connection) {
        executor.submit(new InsertDataJob(insert, connection));
    }

    private String createInsert(List<Map<String, String>> objects) {
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
                } else {
                    sb.append("null");
                }
                sb.append(",");
            }
            deleteLastChar(sb);
            sb.append("),");
        }
        deleteLastChar(sb);
        return sb.toString();
    }

    private void deleteLastChar(StringBuilder sb) {
        sb.deleteCharAt(sb.length() - 1);
    }
}
