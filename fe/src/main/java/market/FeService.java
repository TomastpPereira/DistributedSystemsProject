package market;

import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

@WebService(endpointInterface = "market.Market")
public class FeService implements Market {
    public FeService(){
    }

    private void sendMessage(
            String msg,
            DatagramSocket socket) {
        try {
            Map<InetAddress, Integer> endpoints = new HashMap<>();
            endpoints.put(InetAddress.getByName(Dotenv.load().get("FE_SERVICE_IP")), Integer.parseInt(Dotenv.load().get("FE_SERVICE_PORT")));
            UDPMessage udpMessage = new UDPMessage(UDPMessage.MessageType.CLIENT_REQUEST, msg.split("::")[0], 0, endpoints, msg);
            byte[] buffer = udpMessage.serialize();
            InetAddress receiverAddress = InetAddress.getByName(Dotenv.load().get("FE_IP"));
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, receiverAddress, Integer.parseInt(Dotenv.load().get("FE_PORT")));
            socket.send(packet);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private String receiveMessage(DatagramSocket socket){
        byte[] buffer = new byte[4096];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            socket.receive(packet);
            UDPMessage msg = new UDPMessage(packet.getData(), packet.getLength());
            return (String) msg.getPayload();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return "Unable to process";
        }
    }

    @Override
    public String addShare(String shareID, String shareType, int capacity) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "addShare::" + shareID + "::" + shareType + "::" + capacity;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String removeShare(String shareID, String shareType) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "removeShare::" + shareID + "::" + shareType;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String listShareAvailability(String shareType) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "listShareAvailability::" + shareType;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String purchaseShare(String buyerID, String shareID, String shareType, int shareCount, String datemonthyear) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "purchaseShare::" + buyerID + "::" + shareID + "::" + shareType + "::" + shareCount + "::" + datemonthyear;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String swapShares(String buyerID, String oldShareID, String oldShareType, String newShareID, String newShareType) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "swapShares::" + buyerID + "::" + oldShareID + "::" + oldShareType + "::" + newShareID + "::" + newShareType;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String getShares(String buyerID) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "getShares::" + buyerID;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    @Override
    public String sellShare(String buyerID, String shareID, int shareCount) {
        try (DatagramSocket tempSocket = new DatagramSocket()) {
            String msg = "sellShare::" + buyerID + "::" + shareID + "::" + shareCount;
            sendMessage(msg, tempSocket);
            return receiveMessage(tempSocket);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return "Unable to process";
        }
    }

    public static void main(String[] args) {
        String serviceUrl = "http://localhost:8080/feservice";
        Endpoint.publish(serviceUrl, new FeService());
        System.out.println("FEService is published at " + serviceUrl);
    }
}
