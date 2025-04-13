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
        failureCount.put("RM1",0);
        failureCount.put("RM2",0);
        failureCount.put("RM3",0);

        // Begin Active Listener
        startListener();

        // Allows the RM to notify when it's been restart so it can receive data.
        sendHello();
    }

    /**
     * Listener for UDP Messages to the RM. Infinite receive loop.
     */
    public void startListener(){
        new Thread(()-> {
            try (DatagramSocket socket = new DatagramSocket(this.RM_PORT)){
                while (true){
                    byte[] buffer = new byte[4096];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    UDPMessage msg = deserialize(packet.getData(), packet.getLength());

                    // Copy and change to ACK type to resend
                    UDPMessage ackMessage = new UDPMessage(msg);
                    ackMessage.setMessageType(UDPMessage.MessageType.ACK);
                    sendUDPMessage(ackMessage, packet.getAddress(), packet.getPort());

                    handleMessage(msg);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }).start();
    }


    /**
     * Given the contents of the UDP Message, will prompt different actions from the RM.
     * Switch case based on the message type. Requests originating from the client will always be of type REQUEST.
     *
     * @param msg The UDP message which is being handled.
     */
    private void handleMessage(UDPMessage msg){
        switch(msg.getMessageType()){
            case REQUEST:
                handleSequencedRequest(msg);
                break;
            case CRASH_NOTIFICATION:
                String crashedRM = (String) msg.getPayload(); // Payload should be the string name of the failed RM
                sendPing(crashedRM, RM_IP, RM_PORTS.get(crashedRM));
                break;
            case INCORRECT_RESULT_NOTIFICATION:
                String incorrectRM = (String) msg.getPayload();
                processFailure(incorrectRM);
                break;
            case VOTE:
                String payload = (String) msg.getPayload();
                String crashed = payload.split(":")[0];
                String voter = payload.split(":")[1];
                handleVote(crashed, voter);
                break;
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
            case HELLO:
                sendData(msg.getEndpoints());
                break;
            case SYNC:
                ReplicaStateSnapshot snapshot = (ReplicaStateSnapshot) msg.getPayload();
                loadData(snapshot);
                break;
            case CLEAR_VOTE:
                String toClear = (String) msg.getPayload();
                votesForReplica.put(toClear, new HashSet<>());
                failureCount.put(toClear, 0);
                break;

        }
    }

    /**
     * Upon receiving a notification from the Front End that a response was incorrect, will update the failure count for that RM.
     * Once 3 failures have occurred, the RMs will vote for a restart.
     *
     * @param incorrectRM  The name of the RM which sent the incorrect result.
     */
    private void processFailure(String incorrectRM) {

        int numFails = failureCount.get(incorrectRM);
        numFails++;
        failureCount.put(incorrectRM, numFails);

        // If failed 3 times, send vote out
        // TODO: Adapt to be 3 fails in a row
        if (numFails >= 3){
            voteForRestart(incorrectRM);
        }

    }

    /**
     * Helper for sending UDP messages
     *
     * @param msg The UDP Message being sent
     * @param destAddress   The IP of the receiver
     * @param destPort      The port of the receiver
     */
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

    /**
     * Helper for serializing messages before sending.
     *
     * @param msg The UDP message to serialize
     * @return The byte array of the message
     * @throws IOException  When the Output stream fails.
     */
    private byte[] serialize(UDPMessage msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(msg);
        oos.flush();
        return baos.toByteArray();
    }

    /**
     * Helper to deserialize a UDP Message
     * @param data The byte array that has been received
     * @param length    The length of the byte array
     * @return  The UDP message which has been received
     * @throws IOException  If class is not found during deserialization
     */
    private UDPMessage deserialize(byte[] data, int length) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (UDPMessage) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Class not found during deserialization", e);
        }
    }

    /**
     * When the RM received a Request from the sequencer, this method will be called.
     * If the sequence number of the message matches the expected one, it will be processed right away.
     * If the sequence number is ahead of the expected, it will be put into the holdback queue to ensure total ordering.
     * @param msg The UDP message containing the request.
     */
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
                // Avoids duplicates in the holdback queue
                if (!holdbackQueue.containsKey(sequenceNum))
                    holdbackQueue.put(sequenceNum, msg);
            } else {
                //If an old message was received, it was either already sent or the FE will notify.
            }

        }
    }

    /**
     * Begins the processing of the request, sending it to the markets
     * @param msg UDPMessage containing the data of the request
     */
    //TODO: THIS MUST ALIGN WITH THE FORMAT OF MESSAGES BEING SENT
    // Assumes a request to the markets is of type REQUEST, and payload is MARKET_NAME:param1:param2:etc
    // Action is the type of request, ex purchaseShare, swapShare
    // MARKET_NAME should be NY, LON or TOK
    private void deliver(UDPMessage msg){

        Object[] requestData = (Object[]) msg.getPayload();
        String marketName = (String) requestData[0];

        // Send data should just be param1:param2:etc
        Object[] sendData = new Object[requestData.length - 1];
        for (int i = 1; i < requestData.length; i++) {
            sendData[i - 1] = requestData[i];
        }

        UDPMessage forwardMessage = new UDPMessage(UDPMessage.MessageType.REQUEST, msg.getAction(), 0, null, sendData);

        InetAddress address;
        int marketPort = markets.get(marketName);
        try {
            address = InetAddress.getByName(RM_IP);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        sendUDPMessage(forwardMessage, address, marketPort);
    }

    /**
     * Sends a ping to the given RM to ensure that it has not crashed.
     * The sender will wait for 5000ms to receive a response, and will otherwise suspect a crash.
     *
     * @param replicaName   The name of the crashed RM. Used for voting purposes
     * @param ip            IP of the crashed RM.
     * @param port          Port of the crashed RM.
     */
    public void sendPing(String replicaName, String ip, int port){
        try (DatagramSocket socket = new DatagramSocket()){
            socket.setSoTimeout(5000);

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

    /**
     * Handles the voting for restart of an RM.
     * This method will locally add the vote to the set of votes for the crashed replica.
     * It will then send the vote to the other RMs, avoiding the one that had crashed.
     *
     * @param crashedName The name of the crashed RM, used of the keys of the map.
     */
    private void voteForRestart(String crashedName) {

        // Adding local vote
        Set<String> currentSet = votesForReplica.get(crashedName);
        currentSet.add(RM_NAME);
        votesForReplica.put(crashedName, currentSet);

        // Sending vote out
        UDPMessage vote = new UDPMessage(UDPMessage.MessageType.VOTE, null, 0, null, crashedName+":"+RM_NAME);

        for (Map.Entry<String, Integer> entry: RM_PORTS.entrySet()){
            String rmName = entry.getKey();
            int port = entry.getValue();

            InetAddress address;
            if (!rmName.equals(crashedName) && !rmName.equals(RM_NAME)){
                try {
                    address = InetAddress.getByName(RM_IP);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                sendUDPMessage(vote, address, port);
            }
        }
    }

    /**
     * Handles a vote received from another RM.
     * @param crashed   Name of the crashed RM.
     * @param voter     Name of the RM sending the vote.
     */
    private void handleVote(String crashed, String voter){
        Set<String> currentSet = votesForReplica.getOrDefault(crashed, new HashSet<>());
        currentSet.add(voter);

        int totalVotes = currentSet.size();
        int majority = (RM_PORTS.size() / 2) + 1;

        // Restart if majority is reached - Can't be done with a message
        // This implementation requires RMs to be on the same machine
        if(totalVotes >= majority){
            // Assigns 1 RM which will call to restart based on the voters
            String responsibleRM = currentSet.stream().sorted().findFirst().orElse(null);
            if (responsibleRM.equals(RM_NAME)) {

                // Tell others to clear voting for the restarted RM
                UDPMessage clearVotes = new UDPMessage(UDPMessage.MessageType.CLEAR_VOTE, null, 0, null, crashed);
                try {
                    for (Map.Entry<String, Integer> entry : RM_PORTS.entrySet()) {
                        String otherRM = entry.getKey();
                        int otherPort = entry.getValue();

                        if (!otherRM.equals(RM_NAME)) { // Don't send to self
                            InetAddress address = InetAddress.getByName("localhost");

                            sendUDPMessage(clearVotes, address, otherPort);

                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                restart(crashed);
            }
        }

    }

    /**
     * Sends the data held by this RM to the RM that has requested it.
     * This is used to recover from failure and return to the current state.
     *
     * @param endpoints The IP and Port info to send the data to
     */
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

    /**
     * Loads the data received from another RM.
     *
     * @param snapshot A ReplicaStateSnapshot object which hold all the data to be loaded.
     */
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

    /**
     * Restarts the RM.
     */
    private void restart(String crashed){
        try {
            String javaBin = System.getProperty("java.home") + "/bin/java";

            String classPath = System.getProperty("java.class.path");

            String className = this.getClass().getName();

            int port = RM_PORTS.get(crashed);

            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp", classPath,
                    className,
                    this.RM_IP,
                    String.valueOf(port),
                    crashed
            );

            builder.inheritIO();
            builder.start();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Part of the startup process of an RM.
     * It will send a Hello message to the other RMs in order to get the current data.
     */
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
