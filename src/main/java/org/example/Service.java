package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import org.postgresql.jdbc.PgConnection;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class Service {

    private static final String QUEUE_NAME = "db_changes";
    private static final String RABBITMQ_HOST = "rabbitmq_service_address";
    private static final String PG_LISTEN_CHANNEL = "data_changes";

    public static void main(String[] args) {
        try {

            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "secret");
            PgConnection pgConn = (PgConnection) DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", props);
            Statement stmt = pgConn.createStatement();
            stmt.execute("LISTEN " + PG_LISTEN_CHANNEL);

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
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