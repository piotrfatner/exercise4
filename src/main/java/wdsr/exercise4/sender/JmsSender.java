package wdsr.exercise4.sender;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Enumeration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin2.message.EventMessage;
import wdsr.exercise4.Order;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.activemq.ActiveMQConnectionFactory;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		this.connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		Order order = new Order(orderId,product,price);
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
			Destination destinationQueue = session.createQueue(queueName);

			MessageProducer messageProducer = session.createProducer(destinationQueue);
			ObjectMessage objectMessage = session.createObjectMessage(order);
			objectMessage.setJMSType("Order");
			objectMessage.setStringProperty("WDSR-System", "OrderProcessor");
			messageProducer.send(objectMessage);
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		// TODO
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		// TODO
	}
}
