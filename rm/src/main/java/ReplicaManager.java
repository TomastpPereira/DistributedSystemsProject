import network.UDPMessage;
import market.MarketStateSnapshot;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaManager {

    int RM_PORT;
    String RM_IP;
    String RM_NAME;
    Map<InetAddress, Integer> RETURN_INFO;
    CentralRepositoryServer centralRepo;
    LondonServer londonServer;
    NYServer nyServer;
    TokyoServer tokyoServer;

    long expectedSequence;
    private  Map<Long, UDPMessage> holdbackQueue;
    private PriorityQueue<String> deliveryQueue;
    private Map<String, Set<String>> votesForReplica;
    private Map<String, Integer> failureCount;
    private Map<String, Integer> markets;

    private Map<String, Integer> RM_PORTS = new HashMap<String, Integer>(){{put("RM1", 7001); put("RM2", 7002);put("RM3", 7003);}};

    /**
     * Initializes the Replica Manager on a given port.
     * RMs will be at ports 7001, 7002, 7003 as per the FE implementation.
     *
     * @param ip Ip of the server system, provided as a string.
     * @param port Port of the replica manager, defines the ports of the replicas associated with it.
     */
    public ReplicaManager(String ip, int port, String name){

        // LAUNCHING RM
        RM_PORT = port;
        RM_IP = ip;
        RM_NAME = name;
        RETURN_INFO = new HashMap<>();
        try {
            RETURN_INFO.put(InetAddress.getByName(RM_IP), RM_PORT);
        } catch (Exception e){
            e.printStackTrace();
        }

        // Launching Replica
            // Central Repos will be at 7011, 7012, 7013
        centralRepo = new CentralRepositoryServer(ip, port+10);
        markets.put("Central", port+10);
            // London will be at 7021, 7022, 7023
        londonServer = new LondonServer(ip, port+20);
        markets.put("LON", port+20);
            // London will be at 7031, 7032, 7033
        nyServer = new NYServer(ip, port+30);
        markets.put("NY", port+30);
            // London will be at 7041, 7042, 7043
        tokyoServer = new TokyoServer(ip, port+40);
        markets.put("TOK", port+40);

        // Initializing Necessary Structures
        expectedSequence = 0; //or 1?
        holdbackQueue = new HashMap<>();
        deliveryQueue = new PriorityQueue<>();
        votesForReplica = new ConcurrentHashMap<>();
        votesForReplica.put("RM1", new HashSet<String>());
        votesForReplica.put("RM2", new HashSet<String>());
        votesForReplica.put("RM3", new HashSet<String>());

        // Begin Active Listener
        startListener();

        // Allows the RM to notify when it's been restart so it can receive data.
        sendHello();
    }

    public void startListener(){
        new Thread(()-> {
            try (DatagramSocket socket = new DatagramSocket(this.RM_PORT)){
                while (true){
                    byte[] buffer = new byte[4096];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    UDPMessage msg = deserialize(packet.getData(), packet.getLength());

                    // TODO: SEND ACK
                    handleMessage(msg);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }).start();
    }

    private void handleMessage(UDPMessage msg){
        switch(msg.getMessageType()){
            case SEQUENCED_REQUEST:
                handleSequencedRequest(msg);
                break;
            case ACK:
                break;
            case FAILURE_NOTIFICATION: // DOES THIS CONSIDER BOTH TYPES?
                String failedRM = (String) msg.getPayload(); // Payload should be the string name of the failed RM
                sendPing(failedRM, RM_IP, RM_PORTS.get(failedRM));
                break;
            case VOTE:

            case PING:
                UDPMessage pong = new UDPMessage(UDPMessage.MessageType.PONG, null, 0, null, null);
                InetAddress address = null;
                try {
                    address = InetAddress.getByName(RM_IP);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                int port = msg.getEndpoints().get(address);
                sendUDPMessage(pong, address, port);
                break;
            case RESTART:
                restart();
                break;
            case HELLO:
                sendData(msg.getEndpoints());
                break;
            case SYNC:
                ReplicaStateSnapshot snapshot = (ReplicaStateSnapshot) msg.getPayload();
                loadData(snapshot);
                break;

        }
    }

    private void sendUDPMessage(UDPMessage msg, InetAddress destAddress, int destPort){
        try {
            byte[] data = serialize(msg);
            DatagramPacket packet = new DatagramPacket(data, data.length, destAddress, destPort);
            DatagramSocket socket = new DatagramSocket(RM_PORT);
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private byte[] serialize(UDPMessage msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(msg);
        oos.flush();
        return baos.toByteArray();
    }
    private UDPMessage deserialize(byte[] data, int length) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (UDPMessage) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Class not found during deserialization", e);
        }
    }

    private void handleSequencedRequest(UDPMessage msg){
        synchronized (this){

            long sequenceNum = msg.getSequenceNumber();

            if (sequenceNum == expectedSequence){
                deliver(msg);
                expectedSequence++;

                // Push any messages that can go now
                while (holdbackQueue.containsKey(expectedSequence)) {
                    UDPMessage nextMessage = holdbackQueue.remove(expectedSequence);
                    deliver(nextMessage);
                    expectedSequence++;
                }

            } else if (sequenceNum > expectedSequence) {
                holdbackQueue.put(sequenceNum, msg);
            } else {
                //TODO: handle an old message
            }



        }
    }

    public void sendPing(String replicaName, String ip, int port){
        try (DatagramSocket socket = new DatagramSocket()){
            socket.setSoTimeout(5000); // TODO: Adjust time

            InetAddress address = InetAddress.getByName(ip);

            UDPMessage ping = new UDPMessage(UDPMessage.MessageType.PING, null, 0, null, null);
            sendUDPMessage(ping, address, port);

            byte[] buffer = new byte[4096];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);

            UDPMessage udpResponse = deserialize(response.getData(), response.getLength());

            if (udpResponse.getMessageType().equals(UDPMessage.MessageType.PONG)){
                // Received
            }

        } catch (SocketTimeoutException e) {
            voteForRestart(replicaName);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void sendData(Map<InetAddress, Integer> endpoints){

        try {
            DatagramSocket socket = new DatagramSocket();
            for (String market: markets.keySet()){
                int port = markets.get(market);
                InetAddress address = InetAddress.getByName(RM_IP);

                UDPMessage request = new UDPMessage(UDPMessage.MessageType.REQUEST, "DATA-SEND", 0, null, null);
                sendUDPMessage(request, address, port);
            }

            ReplicaStateSnapshot replicaSnapshot = new ReplicaStateSnapshot();
            int responsesReceived = 0;

            while(responsesReceived < 3){
                try {
                    byte[] buffer = new byte[4096];
                    DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                    socket.receive(response);

                    ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    UDPMessage udpResponse = (UDPMessage) ois.readObject();

                    if (udpResponse.getAction().equals("DATA")){
                        MarketStateSnapshot marketSnapshot = (MarketStateSnapshot) udpResponse.getPayload();
                        String fromMarket = marketSnapshot.getMarket();
                        replicaSnapshot.put(fromMarket, marketSnapshot);
                        responsesReceived++;
                    }
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            socket.close();

            UDPMessage syncMessage = new UDPMessage(UDPMessage.MessageType.SYNC, null, 0, null, replicaSnapshot);
            InetAddress address = (InetAddress) endpoints.keySet().toArray()[0];
            sendUDPMessage(syncMessage, address, endpoints.get(address));

        } catch (Exception e){
            e.printStackTrace();
        }

    }

    private void loadData(ReplicaStateSnapshot snapshot){
        try {
            for (String market: markets.keySet()){
                MarketStateSnapshot marketSnapshot = snapshot.getMarketSnapshots().get(market);

                InetAddress address = InetAddress.getByName(RM_IP);
                int port = markets.get(market);

                UDPMessage helloMsg = new UDPMessage(UDPMessage.MessageType.REQUEST, "DATA-RECEIVE", 0,
                        RETURN_INFO, marketSnapshot);
                sendUDPMessage(helloMsg, address, port);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void restart(){
        try {
            String javaBin = System.getProperty("java.home") + "/bin/java";

            String classPath = System.getProperty("java.class.path");

            String className = this.getClass().getName();

            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp", classPath,
                    className,
                    this.RM_IP,
                    String.valueOf(this.RM_PORT),
                    this.RM_NAME
            );

            builder.inheritIO();
            builder.start();

            System.exit(0);
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    private void sendHello(){
        try{
            for (Map.Entry<String, Integer> entry: RM_PORTS.entrySet()){
                if (!entry.getKey().equals(RM_NAME)){
                    InetAddress address = InetAddress.getByName(RM_IP);
                    int port = entry.getValue();

                    UDPMessage helloMsg = new UDPMessage(UDPMessage.MessageType.HELLO, "hello", 0,
                            RETURN_INFO, "Requesting Data Recovery");
                    sendUDPMessage(helloMsg, address, port);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }




}
