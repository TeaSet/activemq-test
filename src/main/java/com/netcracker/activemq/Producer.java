package com.netcracker.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class Producer {

    private final Logger log = LoggerFactory.getLogger(Producer.class);

    private String clientId;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    public void create(String clientId, String queueName) throws JMSException {

        this.clientId = clientId;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);

        connection = connectionFactory.createConnection();
        connection.setClientID(clientId);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(queueName);

        messageProducer = session.createProducer(queue);
    }

    public void closeConnection() throws JMSException {
        connection.close();
    }

    public void sendName(String firstName, String lastName) throws JMSException {
        String text = firstName + " " + lastName;
        TextMessage message = session.createTextMessage(text);

        messageProducer.send(message);

        log.debug(clientId + ": sent message with text='{}'", text);
    }
}
