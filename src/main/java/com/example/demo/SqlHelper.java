package com.example.demo;

import java.util.List;
import java.util.Map;

public class SqlHelper {

    public static String createInsert(List<Map<String, String>> objects) {
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

    private static void deleteLastChar(StringBuilder sb) {
        sb.deleteCharAt(sb.length() - 1);
    }
}
