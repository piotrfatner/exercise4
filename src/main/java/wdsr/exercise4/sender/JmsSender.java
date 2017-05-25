package wdsr.exercise4.sender;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.Order;

import javax.jms.*;
import java.math.BigDecimal;
import java.util.Map;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private MessageProducer messageProducer;
	private TopicConnection topicConnection;
	private TopicSession topicSession;
	private Topic topic;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		this.connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");

		try {
			connection = connectionFactory.createConnection();
			topicConnection = connectionFactory.createTopicConnection();
			connection.start();
			session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
			topicConnection.start();
			topicSession = topicConnection.createTopicSession(false,TopicSession.AUTO_ACKNOWLEDGE);
			Destination destinationQueue = session.createQueue(queueName);
			topic = topicSession.createTopic(topicName);

			messageProducer = session.createProducer(destinationQueue);
			messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		} catch (JMSException e) {
			e.printStackTrace();
		}
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
		try {
			TextMessage textMessage = session.createTextMessage(text);
			messageProducer.send(textMessage);
			session.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {

			MapMessage mapMessage = topicSession.createMapMessage();
			for (Map.Entry<String,String> entry:map.entrySet()
				 ) {
				mapMessage.setObject(entry.getKey(), entry.getValue());
			}
			messageProducer = session.createProducer(topic);
			messageProducer.send(mapMessage);
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}


}
