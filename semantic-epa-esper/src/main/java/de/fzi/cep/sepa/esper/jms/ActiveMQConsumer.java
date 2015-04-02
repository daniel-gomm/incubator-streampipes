package de.fzi.cep.sepa.esper.jms;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.util.ByteSequence;

public class ActiveMQConsumer extends ActiveMQConnectionProvider implements AutoCloseable {

	 private final Session session;
	 private final MessageConsumer consumer;

	    public ActiveMQConsumer(String url, String topicName) {
	        try {
	            session = startJmsConnection(url).createSession(false, Session.AUTO_ACKNOWLEDGE);
	            consumer = session.createConsumer(session.createTopic(topicName));
	        } catch (JMSException e) {
	            throw new AssertionError(e);
	        }
	    }

	    public void setListener(final IMessageListener listener) {
	        try {
	            consumer.setMessageListener(new MessageListener() {
	                @Override
	                public void onMessage(Message message) {
	                    if (message instanceof TextMessage) {
	                        try {
	                            String json = ((TextMessage) message).getText();
	                            listener.onEvent(json);
	                        } catch (JMSException e) {
	                            throw new IllegalStateException(e);
	                        }
	                    }
	                    if (message instanceof BytesMessage) {
	                        ByteSequence bs = ((ActiveMQBytesMessage) message).getContent();
	                        try {
	                            String json = new String(bs.getData(), "UTF-8");
	                            listener.onEvent(json);
	                        } catch (UnsupportedEncodingException ex) {
	                            Logger.getLogger(ActiveMQConsumer.class.getName()).log(Level.SEVERE, null, ex);
	                        }
	                    }

	                }
	            });
	        } catch (JMSException e) {
	            throw new IllegalStateException(e);
	        }
	    }

	    @Override
	    public void close() throws JMSException {
	        consumer.close();
	        session.close();
	    }
}
