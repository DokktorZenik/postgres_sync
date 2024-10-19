package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import org.postgresql.jdbc.PgConnection;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Service {

    private static final String QUEUE_NAME = "db_changes";
    private static final String RABBITMQ_HOST = System.getenv("RABBITMQ_HOST");
    private static final String PG_LISTEN_CHANNEL = "data_changes";
    private static final String POSTGRES_URL = System.getenv("POSTGRES_URL");;
    public static final String RABBITMQ_USERNAME = System.getenv("RABBITMQ_USERNAME");
    public static final String RABBITMQ_PASSWORD = System.getenv("RABBITMQ_PASSWORD");

    public static void main(String[] args) {
        try {

            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "postgres");
            PgConnection pgConn = (PgConnection) DriverManager.getConnection(POSTGRES_URL, props);
            Statement stmt = pgConn.createStatement();
            stmt.execute("LISTEN " + PG_LISTEN_CHANNEL);

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
            factory.setUsername(RABBITMQ_USERNAME);
            factory.setPassword(RABBITMQ_PASSWORD);
            try (Connection rabbitConnection = factory.newConnection();
                 Channel channel = rabbitConnection.createChannel()) {

                channel.queueDeclare(QUEUE_NAME, false, false, false, null);

                System.out.println("Waiting for notifications from PostgreSQL...");


                while (true) {

                    pgConn.createStatement().execute("SELECT 1");
                    Arrays.stream(pgConn.getNotifications()).forEach(notification -> {
                        try {

                            String message = notification.getParameter();
                            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                            System.out.println("Sent message to RabbitMQ: " + message);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });

                    Thread.sleep(500);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}