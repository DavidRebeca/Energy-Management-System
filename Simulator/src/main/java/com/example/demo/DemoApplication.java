package com.example.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class DemoApplication {

	public static void main(String[] args) {

		String hostname = "localhost";
		int port = 5672; // Default AMQPS port
		String username = "guest";
		String password = "guest";
		String virtualHost = "/";

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(hostname);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
		factory.setVirtualHost(virtualHost);

		Connection connection = null;
		Channel channel = null;

		try {
			// Create connection and channel
			connection = factory.newConnection();
			channel = connection.createChannel();

			// Declare exchange
			String exchangeName = "mesurments";
			channel.exchangeDeclare(exchangeName, "fanout");

			// Declare queue
			String queue = "simulator";
			boolean durable = false;
			boolean exclusive = false;
			boolean autoDelete = false;
			channel.queueDeclare(queue, durable, exclusive, autoDelete, null);

			// Bind queue to exchange
			channel.queueBind(queue, exchangeName, "");

			// Publish messages
			LocalDateTime localDateTime = LocalDateTime.now();

			// Convert local date and time to timestamp
			long timestamp = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
			File file = new File("C:\\Users\\Rebeca\\IdeaProjects\\SD\\Simulator\\sensor.csv");

			try (Scanner scan = new Scanner(file)) {
				while (scan.hasNextLine()) {
					timestamp=timestamp+600;
					String value = scan.nextLine();
					System.out.println(value);
					JSONObject obj = new JSONObject();
					obj.put("id_sensor", "83bd843c-6cd4-43ae-86a2-a556bd4fce0f");
					obj.put("timestamp", String.valueOf(timestamp));
					obj.put("value", value);

					System.out.println(obj);
					// Publish message to the exchange
					channel.basicPublish(exchangeName, "simulator", null, obj.toString().getBytes(StandardCharsets.UTF_8));

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		} finally {
			try {
				if (channel != null && channel.isOpen()) {
					channel.close();
				}
				if (connection != null && connection.isOpen()) {
					connection.close();
				}
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
			}
		}
	}
}
