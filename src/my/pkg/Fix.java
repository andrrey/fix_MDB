package my.pkg;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.jms.*;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Enumeration;

/**
 * (c) Andrey Semenyuk 2015
 * This MDB will process every incoming message and delete all non-alphanumeric ascii-characters
 */

@MessageDriven(
        mappedName="jms/input_q",
        name = "Fix_MDB"
)
public class Fix implements MessageListener {
    @Resource
    private MessageDrivenContext mdc;

    private final Logger log = Logger.getLogger(Fix.class);

    @Resource(name="jms/ConnFactory")
    private ConnectionFactory connectionFactory;

    @Resource(name="jms/output_q")
    private Destination destination;

    Connection connection = null;
    Session session = null;

    private void logE(Exception e){
        String msg ="Exception: " + e.getClass().getName() + "\n" + e.getMessage() + "\n";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        msg+=sw.toString();

        log.error(msg);
    }

    public void onMessage(Message message) {
        try{
            log.debug("Message " + message.getJMSMessageID() + " received");
            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

            if(message instanceof TextMessage){
                TextMessage textMessage = (TextMessage) message;
                sendMessage(processMessage(textMessage), message.getJMSDeliveryMode(), message.getJMSPriority(), TextMessage.DEFAULT_TIME_TO_LIVE);
            } else{
                log.debug("Message is not a TextMessage, so it will be sent without any processing");
                sendMessage(message, message.getJMSDeliveryMode(), message.getJMSPriority(), Message.DEFAULT_TIME_TO_LIVE);
            }

        } catch (Exception e){
            logE(e);
            mdc.setRollbackOnly();
        }

        finally {
            try {
                session.close();
            }catch (Exception e){
                logE(e);
            }
        }

    }

    @PreDestroy
    private void destroy() {
        try {
            if(null != connection) connection.close();
        } catch (Exception e){
            logE(e);
        }
    }

    @PostConstruct
    private void init() {
        try {
            if (null == connection) connection = connectionFactory.createConnection();
        } catch (Exception e){
            logE(e);
        }

    }

    private void sendMessage(Message message, int deliveryMode, int prio, long ttl) throws JMSException{
        MessageProducer producer = session.createProducer(destination);
        producer.send(message, deliveryMode, prio, ttl);
    }

    private TextMessage processMessage(TextMessage inMsg) throws JMSException{
        log.debug("Will process message now. Message text before processing: " + inMsg.getText());
        StringBuilder sb = new StringBuilder();

        for(char c: inMsg.getText().toCharArray()){
            if((c=='\t') || (c=='\r') || (c=='\n') || ((c>=32) && (c<=126)))
                sb.append(c);
        }

        TextMessage outMsg = session.createTextMessage(sb.toString());

        Enumeration props = inMsg.getPropertyNames();

        log.debug("Copy all message properties");
        while(props.hasMoreElements()){
            String propName = (String) props.nextElement();
            log.debug("Copy property " + propName);
            outMsg.setObjectProperty(propName, inMsg.getObjectProperty(propName));
        }

        log.debug("Set ReplyTo to " + inMsg.getJMSReplyTo() + " as in original message");
        outMsg.setJMSReplyTo(inMsg.getJMSReplyTo());

        log.debug("Message text after processing: " + outMsg.getText());

        return outMsg;
    }
}
