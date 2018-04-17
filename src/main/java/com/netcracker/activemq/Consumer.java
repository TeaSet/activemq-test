package com.netcracker.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class Consumer {

    private final Logger log = LoggerFactory.getLogger(Consumer.class);

    final String NO_GREETING = "no greeting";

    private String clientId;
    private Connection connection;
    private MessageConsumer messageConsumer;

    public void create(String clientId, String queueName) throws JMSException {

        this.clientId = clientId;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);

        connection = connectionFactory.createConnection();
        connection.setClientID(clientId);

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        Queue queue = session.createQueue(queueName);

        messageConsumer = session.createConsumer(queue);

        connection.start();
    }

    public void closeConnection() throws JMSException {
        connection.close();
    }

    public String getGreeting(int timeout, boolean acknowledge)
            throws JMSException {

        String greeting = NO_GREETING;

        // read a message from the queue destination
        Message message = messageConsumer.receive(timeout);

        // check if a message was received
        if (message != null) {
            // cast the message to the correct type
            TextMessage textMessage = (TextMessage) message;

            // retrieve the message content
            String text = textMessage.getText();
            log.debug(clientId + ": received message with text='{}'",
                    text);

            if (acknowledge) {
                // acknowledge the successful processing of the message
                message.acknowledge();
                log.debug(clientId + ": message acknowledged");
            } else {
                log.debug(clientId + ": message not acknowledged");
            }
            // create greeting
            greeting = "Hello " + text + "!";
        } else {
            log.debug(clientId + ": no message received");
        }

        log.info("greeting={}", greeting);
        return greeting;
    }
}
