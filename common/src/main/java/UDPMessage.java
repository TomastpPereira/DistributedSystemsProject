import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

public class UDPMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    // Message types that are commonly used in the project.
    public enum MessageType {
        REQUEST,            // For client requests
        RESPONSE,           // For replica responses
        ACK,                // Acknowledgments for reliability
//        FAILURE_NOTIFICATION, // Reporting incorrect results or crashes
        VOTE,               // For voting during recovery
        PING,                // For checking if a node is alive
        PONG,                // Received after a Ping
        HELLO               // Sent when an RM is restarted
    }

    // Unique identifier for this message.
    private String messageId;

    // The type of message (REQUEST, RESPONSE, etc.)
    private MessageType messageType;

    // The action initiated by the client (e.g., "purchaseShare", "sellShare").
    private String action;

    // The number of times this message has been retried.
    private int retry;

    // Instead of separate requestId and senderId, we store a map of endpoints.
    // The key is the InetAddress and the value is the port number.
    private Map<InetAddress, Integer> endpoints;

    // The payload contains the main content of the message.
    private Object payload;

    // Timestamp when the message was created.
    private long timestamp;

    //Sequence Number
    private long sequenceNumber;

    // Constructor
    public UDPMessage(MessageType messageType, String action, int retry,
                      Map<InetAddress, Integer> endpoints, Object payload) {
        this.messageType = messageType;
        this.messageId = UUID.randomUUID().toString();
        this.action = action;
        this.retry = retry;
        this.endpoints = endpoints;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public UDPMessage(MessageType messageType, String action, int retry,
                      Map<InetAddress, Integer> endpoints, long sequenceNumber, Object payload) {
        this.messageType = messageType;
        this.messageId = UUID.randomUUID().toString();
        this.action = action;
        this.retry = retry;
        this.endpoints = endpoints;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
        this.sequenceNumber = sequenceNumber;
    }


    // Getters and Setters
    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public Map<InetAddress, Integer> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Map<InetAddress, Integer> endpoints) {
        this.endpoints = endpoints;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UDPMessage{" +
                "messageId='" + messageId + '\'' +
                ", messageType=" + messageType +
                ", action='" + action + '\'' +
                ", retry=" + retry +
                ", endpoints=" + endpoints +
                ", payload=" + payload +
                ", timestamp=" + timestamp +
                '}';
    }
}
