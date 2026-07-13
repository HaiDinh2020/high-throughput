package org.example.database;

import org.example.Message;

import java.sql.*;
import java.util.List;

public class DatabaseConnection {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/high_throughput?allowPublicKeyRetrieval=true&useSSL=false&rewriteBatchedStatements=true";

    static final String USER = "user";
    static final String PASS = "password";

    public static Connection getConnection() {

        try {
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);
            System.out.println("Successfully connected!");
//            createTableIfNotExists(connection);
//            clearMessagesTable(connection);
            return connection;
        } catch (SQLException e) {
            System.err.println(" Connection failed: " + e.getMessage());
            throw new RuntimeException("Failed to connect to the database", e);
        }
    }

    private static void createTableIfNotExists(Connection connection) throws SQLException {
        String dropTableSQL = "DROP TABLE IF EXISTS messages";
        String createTableSQL = "CREATE TABLE IF NOT EXISTS messages (" +
                "id VARCHAR(36) PRIMARY KEY," +
                "payload VARCHAR(255)," +
                "created_time BIGINT" +
                ")";
        try (Statement stmt = connection.createStatement()) {
//            stmt.execute(dropTableSQL);
            stmt.execute(createTableSQL);
            connection.commit();
            System.out.println("Table 'messages' is ready.");
        }
    }

    private static void clearMessagesTable(Connection connection) throws SQLException {
        String deleteSQL = "DELETE FROM messages";
        try (Statement stmt = connection.createStatement()) {
            int rowsDeleted = stmt.executeUpdate(deleteSQL);
            connection.commit();
            System.out.println("Cleared 'messages' table, deleted " + rowsDeleted + " rows.");
        }
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Database connection closed.");
            } catch (SQLException e) {
                System.err.println("Failed to close database connection: " + e.getMessage());
            }
        }
    }

    public static void insertMessage(Connection connection, int id, String payload, long createdTime) {
        String insertSQL = "INSERT INTO messages (id, payload, created_time) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setInt(1, id);
            pstmt.setString(2, payload);
            pstmt.setLong(3, createdTime);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Failed to insert message: " + e.getMessage());
        }
    }

    public static void buckInsertMessages(Connection connection, List<Message> messages) {
        String insertSQL = "INSERT INTO messages (id, payload, created_time) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (Message message : messages) {
                pstmt.setString(1, message.getId());
                pstmt.setString(2, message.getPayload());
                pstmt.setLong(3, message.getCreatedTime());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            pstmt.clearBatch();
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Failed to insert messages in batch: " + e.getMessage());
        }
    }
}
