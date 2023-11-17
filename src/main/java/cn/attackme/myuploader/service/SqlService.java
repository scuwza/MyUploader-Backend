package cn.attackme.myuploader.service;

import cn.attackme.myuploader.config.DatabaseConfig;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;


@Service
public class SqlService {
    @Autowired
    private DatabaseConfig databaseConfig;

    // 直接使用配置类中的属性值
    private String jdbcUrl = databaseConfig.jdbcUrl;
    private String jdbcUser = databaseConfig.jdbcUser;
    private String jdbcPassword = databaseConfig.jdbcPassword;

    public void importCSVToDatabase(String csvFilePath) throws IOException, SQLException {
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(Files.newInputStream(Paths.get(csvFilePath)), StandardCharsets.UTF_8))) {
            String[] columnNames = csvReader.readNext();
            List<String> tableHeaders = Arrays.stream(columnNames)
                    .map(String::trim)
                    .filter(name -> !name.isEmpty())
                    .collect(Collectors.toList());

            List<String[]> rows = csvReader.readAll();

            try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
                connection.setAutoCommit(false);
                String tableName = extractTableName(csvFilePath);
                createTable(connection, tableName, tableHeaders, rows);
                String insertQuery = generateInsertQuery(tableName, tableHeaders);
                insertData(connection, insertQuery, tableHeaders, rows);
                connection.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CSV file.", e);
        }
    }

    private void insertData(Connection connection, String insertQuery, List<String> tableHeaders, List<String[]> rows) {
        int batchCount = 0;
        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
            for (String[] data : rows) {
                batchCount++;
                for (int i = 0; i < data.length; i++) {
                    String columnName = tableHeaders.get(i).trim();
                    statement.setString(i + 1, data[i]);
                }
                statement.addBatch();
                if (batchCount % 1000 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch(); // 处理剩余的数据
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert data into the database.", e);
        }
    }

    private String generateInsertQuery(String tableName, List<String> tableHeaders) {
        List<String> escapedColumns = tableHeaders.stream()
                .map(column -> String.format("`%s`", column))
                .collect(Collectors.toList());
        String columns = String.join(", ", escapedColumns);
        String placeholders = tableHeaders.stream().map(header -> "?").collect(Collectors.joining(", "));
        return String.format("INSERT INTO `%s` (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private void createTable(Connection connection, String tableName, List<String> tableHeaders, List<String[]> rows)
            throws SQLException {
        StringBuilder createTableQuery = new StringBuilder();
        createTableQuery.append("CREATE TABLE `").append(tableName).append("` (");

        for (int i = 0; i < tableHeaders.size(); i++) {
            String columnName = tableHeaders.get(i);
            String columnType = determineColumnType(columnName, rows, tableHeaders);

            createTableQuery.append("`").append(columnName).append("` ").append(columnType);

            if (i < tableHeaders.size() - 1) {
                createTableQuery.append(", ");
            }
        }

        createTableQuery.append(")");

        try (PreparedStatement statement = connection.prepareStatement(createTableQuery.toString())) {
            statement.executeUpdate();
        }
    }

    private String determineColumnType(String columnName, List<String[]> rows, List<String> tableHeaders) {
        boolean isInt = isColumnType(rows, tableHeaders.indexOf(columnName), this::isInteger);
        boolean isDouble = isColumnType(rows, tableHeaders.indexOf(columnName), this::isDouble);
        boolean isDate = isColumnType(rows, tableHeaders.indexOf(columnName), this::isDate);

        if (isInt) {
            return "INT";
        } else if (isDouble) {
            return "DOUBLE";
        } else if (isDate) {
            return "DATE";
        } else {
            return "VARCHAR(255)";
        }
    }

    private boolean isColumnType(List<String[]> rows, int columnIndex, Predicate<String> typeChecker) {
        for (String[] row : rows) {
            String data = row[columnIndex];
            if (!typeChecker.test(data)) {
                return false;
            }
        }
        return true;
    }

    private boolean isInteger(String data) {
        try {
            Integer.parseInt(data);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isDouble(String data) {
        try {
            Double.parseDouble(data);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isDate(String data) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try {
            LocalDate.parse(data, dateFormatter);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private String extractTableName(String csvFilePath) {
        File file = new File(csvFilePath);
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex > 0) ? fileName.substring(0, dotIndex) : fileName;
    }
}

