import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;
import utility.BufferedLog;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class FrontEnd {

    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString())
            .load();

    private final List<InetSocketAddress> replicaManagerEndpoints;
    private final InetSocketAddress sequencerEndpoint;

    private final DatagramSocket socket;
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private static BufferedLog log;

    private static long ACK_TIMEOUT;
    private static long REPLICA_RESPONSE_TIMEOUT;

    private sealed interface MessageState permits Received, Sending, WaitForAck, WaitToSequence, Sequenced, WaitToDie, Dead {
    }

    private final class Received implements MessageState {
        InetSocketAddress address;

        Received(InetSocketAddress address) {
            this.address = address;
        }
    }

    private final class Sending implements MessageState {
        InetSocketAddress remoteAddress;

        Sending(InetSocketAddress address) {
            this.remoteAddress = address;
        }
    }

    private final class WaitForAck implements MessageState {
        long sendTime;
        InetSocketAddress remoteAddress;

        WaitForAck(InetSocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
            this.sendTime = System.currentTimeMillis();
        }
    }

    private final class WaitToSequence implements MessageState {
        InetSocketAddress address;

        WaitToSequence(InetSocketAddress address) {
            this.address = address;
        }
    }

    private final class Sequenced implements MessageState {
        long sequenceNumber;
        InetSocketAddress clientAddress;
        HashMap<Integer, String> results;
        long creationTime;

        Sequenced(long sequenceNumber, InetSocketAddress clientAddress) {
            this.sequenceNumber = sequenceNumber;
            this.clientAddress = clientAddress;
            this.results = new HashMap<>();
            this.creationTime = System.currentTimeMillis();
        }

        public void addResult(int port, String result) {
            results.put(port, result);
        }

        public boolean isFull() {
            return results.size() == 3;
        }
    }

    private final class WaitToDie implements MessageState {
        long sequenceNumber;
        InetSocketAddress clientAddress;
        HashMap<Integer, String> results;
        long creationTime;

        WaitToDie(Sequenced sc) {
            this.sequenceNumber = sc.sequenceNumber;
            this.clientAddress = sc.clientAddress;
            this.results = sc.results;
            this.creationTime = sc.creationTime;
        }

        public void addResult(int port, String result) {
            results.put(port, result);
        }

        public boolean isFull() {
            return results.size() == 3;
        }
    }

    private final class Dead implements MessageState {
    }

    private class Message {
        UDPMessage message;
        MessageState state;

        Message(UDPMessage message, MessageState state) {
            this.message = message;
            this.state = state;
        }

        public void setState(MessageState state) {
            this.state = state;
        }
    }

    private void sendMessage(UDPMessage message, InetAddress remoteAddress, int remotePort) {
        try {
            byte[] buffer = message.serialize();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, remoteAddress, remotePort);
            socket.send(packet);
            log.logEntry("FE_SendLoop", "Message Sent", BufferedLog.RequestResponseStatus.SUCCESS,
                    "MessageID: " + message.getMessageId(), "Sent to " + remoteAddress);
            System.out.println("Send message to " + remoteAddress + ":" + remotePort + " " + message);
        } catch (Exception e) {
            log.logEntry("FE_SendLoop", "Error Sending Message", BufferedLog.RequestResponseStatus.FAILURE,
                    e.getMessage(), "Endpoint: " + remoteAddress);
            e.printStackTrace(System.err);
        }
    }

    public FrontEnd() throws Exception {
        log = new BufferedLog("logs/", "Front-End", "Front-End");

        this.replicaManagerEndpoints = new ArrayList<>();
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_ONE_IP")), Integer.parseInt(dotenv.get("RM_ONE_PORT"))));
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_TWO_IP")), Integer.parseInt(dotenv.get("RM_TWO_PORT"))));
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_THREE_IP")), Integer.parseInt(dotenv.get("RM_THREE_PORT"))));
        this.sequencerEndpoint = new InetSocketAddress(InetAddress.getByName(dotenv.get("SEQUENCER_IP")), Integer.parseInt(dotenv.get("SEQUENCER_PORT")));
        ACK_TIMEOUT = Long.parseLong(dotenv.get("ACK_TIMEOUT"));
        REPLICA_RESPONSE_TIMEOUT = Long.parseLong(dotenv.get("REPLICA_RESPONSE_TIMEOUT"));

        this.socket = new DatagramSocket(Integer.parseInt(dotenv.get("FE_PORT")), InetAddress.getByName(dotenv.get("FE_IP")));
        log.logEntry("Constructor", "Initialization complete", BufferedLog.RequestResponseStatus.SUCCESS,
                "FE listening on port " + dotenv.get("FE_PORT"), "Front-End started");
    }

    public void start() {
        new Thread(this::receiveLoop).start();
        new Thread(this::processIncomingMessages).start();
        log.logEntry("FE_Start", "Front-End started", BufferedLog.RequestResponseStatus.SUCCESS, "All threads launched", "");
    }

    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                UDPMessage msg = new UDPMessage(packet.getData(), packet.getLength());
                synchronized (messages) {
                    Message receivedMsg = new Message(msg, new Received(new InetSocketAddress(packet.getAddress(), packet.getPort())));
                    messages.add(receivedMsg);
                }
                log.logEntry("FE_ReceiveLoop", "Message Received", BufferedLog.RequestResponseStatus.SUCCESS,
                        "MessageID: " + msg.getMessageId(), "From: " + packet.getAddress());
                System.out.println("Received message from " + packet.getAddress() + ":" + packet.getPort() + " " + msg);
            } catch (IOException e) {
                log.logEntry("FE_ReceiveLoop", "IOException", BufferedLog.RequestResponseStatus.FAILURE,
                        e.getMessage(), "Error in receiving message");
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void processIncomingMessages() {
        while (true) {
            synchronized (messages) {
                for (Message processingMessage : messages) {
                    if (processingMessage.state instanceof Received r) {
                        if (processingMessage.message.getMessageType() == UDPMessage.MessageType.ACK) {
                            log.logEntry("FE_HandleMessage", "ACK received", BufferedLog.RequestResponseStatus.SUCCESS,
                                    "MessageID: " + processingMessage.message.getMessageId(), "Processing ACK");

                            List<Message> receivedMessages = messages.stream()
                                    .filter(m -> m.message.getMessageId().equals(processingMessage.message.getMessageId()))
                                    .toList();

                            for (Message m : receivedMessages) {
                                m.setState(new Dead());
                            }
                        } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.RESPONSE) {  // From Sequencer
                            log.logEntry("FE_HandleMessage", "Response received", BufferedLog.RequestResponseStatus.SUCCESS,
                                    "Sequence: " + processingMessage.message.getSequenceNumber(), "Processing response");
                            try {
                                Map.Entry<InetAddress, Integer> firstEntry = processingMessage.message.getEndpoints().entrySet().iterator().next();
                                InetSocketAddress clientAddress = new InetSocketAddress(firstEntry.getKey(), firstEntry.getValue());
                                processingMessage.setState(new Sequenced(processingMessage.message.getSequenceNumber(), clientAddress));

                                UDPMessage message = new UDPMessage(processingMessage.message);
                                message.setMessageType(UDPMessage.MessageType.ACK);
                                Message ackMessage = new Message(message, new Sending(r.address));

                            } catch (Exception e) {
                                log.logEntry("FE_HandleMessage", "Error adding to sequencer queue", BufferedLog.RequestResponseStatus.FAILURE,
                                        e.getMessage(), "");
                                System.out.println(e.getMessage());
                            }
                        } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.RESULT) { // From RM
                            log.logEntry("FE_HandleMessage", "Result message received", BufferedLog.RequestResponseStatus.SUCCESS,
                                    "Sequence: " + processingMessage.message.getSequenceNumber(), "Processing result");

                            UDPMessage message = new UDPMessage(processingMessage.message);
                            message.setMessageType(UDPMessage.MessageType.ACK);
                            Message ackMessage = new Message(message, new Sending(r.address));

                            Optional<Message> maybeSequencedMessage = messages.stream()
                                    .filter(m -> m.state instanceof Sequenced s)
                                    .map(m -> Map.entry(m, (Sequenced) m.state))
                                    .filter(entry -> entry.getValue().sequenceNumber == processingMessage.message.getSequenceNumber())
                                    .map(Map.Entry::getKey)
                                    .findFirst();

                            if (maybeSequencedMessage.isPresent()) {
                                String result = (String) processingMessage.message.getPayload();
                                int serverPort = r.address.getPort();
                                Message sequencedMessage = maybeSequencedMessage.get();
                                if (sequencedMessage.state instanceof Sequenced s) {
                                    s.addResult(serverPort, result);
                                }
                                processingMessage.setState(new Dead());
                            } else {
                                processingMessage.setState(new WaitToSequence(r.address));
                            }
                        } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.REQUEST) {// From Client
                            log.logEntry("FE_HandleMessage", "Request message received", BufferedLog.RequestResponseStatus.INFO,
                                    "Processing new request", "Forwarding ACK and sending to Sequencer");
                            try {
                                Message ackMessage = new Message(processingMessage.message, new Sending(sequencerEndpoint));
                            } catch (Exception e) {
                                log.logEntry("FE_HandleMessage", "Exception handling REQUEST", BufferedLog.RequestResponseStatus.FAILURE,
                                        e.getMessage(), "");
                                System.out.println("Exception in Front-End handleUDPMessage " + e.getMessage());
                            }
                            break;

                        } else {
                            log.logEntry("FE_HandleMessage", "Unknown message type", BufferedLog.RequestResponseStatus.DEBUG,
                                    processingMessage.message.toString(), "Received unknown message");
                            System.out.println("Received unknown message type: " + processingMessage);
                        }
                    } else if (processingMessage.state instanceof Sending s) {
                        sendMessage(processingMessage.message, s.remoteAddress.getAddress(), s.remoteAddress.getPort());
                        processingMessage.setState(new WaitForAck(s.remoteAddress));
                    } else if (processingMessage.state instanceof WaitForAck w) {
                        if ((System.currentTimeMillis() - w.sendTime) < ACK_TIMEOUT) {
                            processingMessage.setState(new Sending(w.remoteAddress));
                        }
                    } else if (processingMessage.state instanceof WaitToSequence w) {
                        Optional<Message> maybeSequencedMessage = messages.stream()
                                .filter(m -> m.state instanceof Sequenced s)
                                .map(m -> Map.entry(m, (Sequenced) m.state))
                                .filter(entry -> entry.getValue().sequenceNumber == processingMessage.message.getSequenceNumber())
                                .map(Map.Entry::getKey)
                                .findFirst();

                        if (maybeSequencedMessage.isPresent()) {
                            String result = (String) processingMessage.message.getPayload();
                            int serverPort = w.address.getPort();
                            Message sequencedMessage = maybeSequencedMessage.get();
                            if (sequencedMessage.state instanceof Sequenced s) {
                                s.addResult(serverPort, result);
                            }
                            processingMessage.setState(new Dead());
                        } else {
                            Optional<Message> maybeWaitToDieMessage = messages.stream()
                                    .filter(m -> m.state instanceof WaitToDie s)
                                    .map(m -> Map.entry(m, (WaitToDie) m.state))
                                    .filter(entry -> entry.getValue().sequenceNumber == processingMessage.message.getSequenceNumber())
                                    .map(Map.Entry::getKey)
                                    .findFirst();

                            if (maybeWaitToDieMessage.isPresent()) {
                                String result = (String) processingMessage.message.getPayload();
                                int serverPort = w.address.getPort();
                                Message waitToDieMessage = maybeWaitToDieMessage.get();
                                if (waitToDieMessage.state instanceof WaitToDie d) {
                                    d.addResult(serverPort, result);
                                }
                            }
                        }
                    } else if (processingMessage.state instanceof Sequenced s) {
                        if (s.results.size() >= 2) {
                            Map<String, List<Integer>> groupedByResponse = s.results.entrySet()
                                    .stream()
                                    .collect(Collectors.groupingBy(
                                            Map.Entry::getValue,
                                            Collectors.mapping(Map.Entry::getKey,
                                                    Collectors.toList())
                                    ));

                            if (groupedByResponse.size() == 1) { // All the rms sent same result
                                Map.Entry<String, List<Integer>> entry = groupedByResponse.entrySet().iterator().next();
                                if (entry.getValue().size() == 2) {
                                    String response = entry.getKey();
                                    UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                            processingMessage.message.getAction(), 0,
                                            processingMessage.message.getEndpoints(),
                                            processingMessage.message.getSequenceNumber(),
                                            response
                                    );
                                    messages.add(new Message(message, new Sending(s.clientAddress)));
                                    processingMessage.setState(new WaitToDie(s));
                                } else if (entry.getValue().size() == 3) {
                                    String response = entry.getKey();
                                    UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                            processingMessage.message.getAction(), 0,
                                            processingMessage.message.getEndpoints(),
                                            processingMessage.message.getSequenceNumber(),
                                            response
                                    );
                                    messages.add(new Message(message, new Sending(s.clientAddress)));
                                    processingMessage.setState(new Dead());
                                }
                            } else if (groupedByResponse.size() == 2) { // One rm sent a different result
                                List<Integer> lowestGroup = groupedByResponse.values().stream()
                                        .min(Comparator.comparingInt(List::size))
                                        .orElse(List.of());

                                Map.Entry<String, List<Integer>> highestGroup = groupedByResponse.entrySet()
                                        .stream()
                                        .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                                        .orElse(null);

                                String response = highestGroup.getKey();

                                UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                        processingMessage.message.getAction(), 0,
                                        processingMessage.message.getEndpoints(),
                                        processingMessage.message.getSequenceNumber(),
                                        response
                                );
                                messages.add(new Message(message, new Sending(s.clientAddress)));

                                StringBuilder errorMsg = new StringBuilder();
                                for (Integer addr : lowestGroup) {
                                    errorMsg.append(addr).append("::");
                                }
                                int length = errorMsg.length();
                                if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                    errorMsg.setLength(length - 2);
                                }
                                UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    messages.add(new Message(rmMessage, new Sending(rmEndpoint)));
                                }
                                log.logEntry("FE_CheckResults", "Mismatch detected", BufferedLog.RequestResponseStatus.FAILURE,
                                        errorMsg.toString(), "Notified RM for request " + s.sequenceNumber);
                                processingMessage.setState(new Dead());
                            } else if (groupedByResponse.size() == 3) { // All the rms sent different results
                                List<Integer> unresponsivePorts = replicaManagerEndpoints.stream()
                                        .map(InetSocketAddress::getPort)
                                        .filter(port -> !s.results.containsKey(port))
                                        .toList();

                                StringBuilder errorMsg = new StringBuilder();
                                for (Integer addr : unresponsivePorts) {
                                    errorMsg.append(addr).append("::");
                                }
                                int length = errorMsg.length();
                                if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                    errorMsg.setLength(length - 2);
                                }
                                UDPMessage message = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    messages.add(new Message(message, new Sending(rmEndpoint)));
                                }
                                log.logEntry("FE_CheckResults", "Mismatch detected", BufferedLog.RequestResponseStatus.FAILURE,
                                        errorMsg.toString(), "Notified RM for request " + s.sequenceNumber);
                                processingMessage.setState(new Dead());
                            }

                        } else if ((System.currentTimeMillis() - s.creationTime) < REPLICA_RESPONSE_TIMEOUT) {
                            List<Integer> unresponsivePorts = replicaManagerEndpoints.stream()
                                    .map(InetSocketAddress::getPort)
                                    .filter(port -> !s.results.containsKey(port))
                                    .toList();

                            StringBuilder errorMsg = new StringBuilder();
                            for (Integer addr : unresponsivePorts) {
                                errorMsg.append(addr).append("::");
                            }
                            int length = errorMsg.length();
                            if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                errorMsg.setLength(length - 2);
                            }
                            UDPMessage message = new UDPMessage(UDPMessage.MessageType.CRASH_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                            for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                messages.add(new Message(message, new Sending(rmEndpoint)));
                            }
                            log.logEntry("FE_CheckResults", "Crash detected", BufferedLog.RequestResponseStatus.FAILURE,
                                    errorMsg.toString(), "Notified RM for request " + s.sequenceNumber);
                            processingMessage.setState(new Dead());

                            // TODO : Send Crash Report to Client
                        }
                    } else if (processingMessage.state instanceof WaitToDie w) {
                        if (w.isFull()) {
                            Map<String, List<Integer>> groupedByResponse = w.results.entrySet()
                                    .stream()
                                    .collect(Collectors.groupingBy(
                                            Map.Entry::getValue,
                                            Collectors.mapping(Map.Entry::getKey,
                                                    Collectors.toList())
                                    ));
                            if (groupedByResponse.size() == 1) {
                                processingMessage.setState(new Dead());
                            } else {
                                List<Integer> lowestGroup = groupedByResponse.values().stream()
                                        .min(Comparator.comparingInt(List::size))
                                        .orElse(List.of());

                                StringBuilder errorMsg = new StringBuilder();
                                for (Integer addr : lowestGroup) {
                                    errorMsg.append(addr).append("::");
                                }
                                int length = errorMsg.length();
                                if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                    errorMsg.setLength(length - 2);
                                }
                                UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    messages.add(new Message(rmMessage, new Sending(rmEndpoint)));
                                }
                                log.logEntry("FE_CheckResults", "Mismatch detected", BufferedLog.RequestResponseStatus.FAILURE,
                                        errorMsg.toString(), "Notified RM for request " + w.sequenceNumber);
                                processingMessage.setState(new Dead());
                            }
                        }
                    } else if (processingMessage.state instanceof Dead d) {
                        continue;
                        // TODO: remove msg if necessary
                    }

                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignore) {
            }
        }
    }


    public static void main(String[] args) throws Exception {
        new FrontEnd().start();

        System.out.println("[Front-End] Front-End started.");
        log.logEntry("FE_Main", "Startup complete", BufferedLog.RequestResponseStatus.SUCCESS,
                "Front-End service launched", "Ready to process client requests");
    }
}
