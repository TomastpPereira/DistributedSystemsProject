import network.UDPMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
            // London will be at 7021, 7022, 7023
        londonServer = new LondonServer(ip, port+20);
            // London will be at 7031, 7032, 7033
        nyServer = new NYServer(ip, port+30);
            // London will be at 7041, 7042, 7043
        tokyoServer = new TokyoServer(ip, port+40);

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
                break;
            case VOTE:
                break;
            case PING:
                sendPong();
                break;
            case PONG:
                break;
            case DATA_REQUEST:
                sendData();
                break;
            case RESTART:
                restart();
                break;
            case HELLO;
                sendData();
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

    //TODO
    private void sendPong() {

    }

    private void sendData(){

        ReplicaStateSnapshot toSend = new ReplicaStateSnapshot();


    }

    private void loadData(){

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
