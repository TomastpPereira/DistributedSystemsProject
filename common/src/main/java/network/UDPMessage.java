package network;

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

    public enum MessageType {
        REQUEST,
        RESPONSE,
        ACK,
        VOTE,
        PING,
        PONG,
        HELLO,
        SYNC
    }

    private String messageId;

    private MessageType messageType;

    private String action;

    private int retry;

    private Map<InetAddress, Integer> endpoints;

    private Object payload;

    private long timestamp;

    private long sequenceNumber;

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
        return "network.UDPMessage{" +
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
