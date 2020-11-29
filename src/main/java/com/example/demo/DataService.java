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
    public static final int BATCH_SIZE = 25_000;

    private final Connection connection;
    private final ThreadPoolExecutor executor;

    public DataService(Connection connection) {
        this.connection = connection;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    }

    @SneakyThrows
    public void importData() {
        log.info("Creating insert data");
        List<Map<String, String>> rowsData = new ArrayList<>(BATCH_SIZE);
        for (int i = 1; i <= ROWS; i++) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put(new String("name1"), new String("name1name1name1name1name1"));
            map.put(new String("name2"), new String("name2name2name2name2name2"));
            map.put(new String("name3"), new String("name3name3name3name3name3"));
            map.put(new String("name4"), new String("name4name4name4name4name4"));
            map.put(new String("name5"), new String("name5name5name5name5name5"));
            rowsData.add(map);
            if (i % BATCH_SIZE == 0) {
                String insert = SqlHelper.createInsert(rowsData);
                addImportsToQue(insert, connection);
                rowsData = new LinkedList<>();
                log.info("Batch nr: " + i/BATCH_SIZE + " created.");
            }
        }
        if (!rowsData.isEmpty()) {
            String insert = SqlHelper.createInsert(rowsData);
            addImportsToQue(insert, connection);
        }
        log.info("Creating insert data finished.");
        executor.shutdown();
        log.info("Waiting for all imports");

        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        log.info("All imports finished.");
    }

    private void addImportsToQue(String insert, Connection connection) {
        executor.submit(new InsertDataJob(insert, connection));
    }
}
