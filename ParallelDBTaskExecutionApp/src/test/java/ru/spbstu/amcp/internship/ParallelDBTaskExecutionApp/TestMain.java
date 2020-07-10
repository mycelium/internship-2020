package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import java.sql.*;

public class TestMain {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://127.0.0.1:5432/TestDB";
        String userName = "postgres";
        String password = "root";
        Class.forName("org.postgresql.Driver");

        // Создаем подключение и создаем объект типа Statement
        try(Connection connection = DriverManager.getConnection(url, userName, password);
            Statement stat = connection.createStatement()) {


            String createTable = "CREATE TABLE IF NOT EXISTS Fruit (name VARCHAR(15) NOT NULL, amount INTEGER, price INTEGER NOT NULL, PRIMARY KEY (name))";
            String command1 = "INSERT INTO Fruit (name, amount, price) VALUES ('Apple', 200, 3)";
            String command2 = "INSERT INTO Fruit (name, amount, price) VALUES ('Orange', 50, 50)";
            String command3 = "INSERT INTO Fruit (name, amount, price) VALUES ('Lemon', 50, 450)";
            String command4 = "INSERT INTO Fruit (name, amount, price) VALUES ('Pineapple', 20, 70)";

            connection.setAutoCommit(false);
            stat.executeUpdate("DROP TABLE IF EXISTS FRUIT");
            stat.executeUpdate(createTable);
            stat.executeUpdate(command1);

            Savepoint spt = connection.setSavepoint();
            stat.executeUpdate(command2);
            stat.executeUpdate(command3);
            stat.executeUpdate(command4);

            connection.rollback(spt);
            connection.releaseSavepoint(spt);
            stat.executeUpdate(command3);
            spt = connection.setSavepoint();
            stat.executeUpdate(command4);
            connection.rollback(spt);
            connection.releaseSavepoint(spt);
            connection.commit();


//            connection.setAutoCommit(true);
//            stat.executeUpdate(createTable);
//            stat.addBatch(command1);
//            stat.addBatch(command2);
//            stat.addBatch(command3);
//            stat.addBatch(command4);
//            stat.executeBatch();
        }
    }


}
