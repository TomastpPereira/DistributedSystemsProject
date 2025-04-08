import market.MarketImpl;
import market.MarketStateSnapshot;
import network.UDPMessage;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * UDP Server which allows for the messages to be passed between markets.
 */
public class UDPServer extends Thread{

    private final int port;
    private final MarketImpl market;

    public UDPServer(int port, MarketImpl market){
        this.port = port;
        this.market = market;
    }

    public void run(){
        try (DatagramSocket socket = new DatagramSocket(port)){
            System.out.println("UDP Server connected to port " + port);

            while (true){
                byte[] buffer = new byte[4096];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                ByteArrayInputStream bais = new ByteArrayInputStream(request.getData(), 0, request.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpMessage = (UDPMessage) ois.readObject();

                UDPMessage response = null;

                switch(udpMessage.getAction()){
                    case "SHARES":
                        String buyerID = (String) udpMessage.getPayload();
                        String ownedShares = market.getLocalOwnedShares(buyerID);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "SHARES", 0, null, ownedShares);
                        break;
                    case "CROSS":
                        Object[] crossData = (Object[]) udpMessage.getPayload();
                        market.updateCrossMarket((String) crossData[0], (int) crossData[1], (int) crossData[2]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "CROSS", 0, null, "Success");
                        break;
                    case "BUY_CHECK":
                        Object[] buyCheckData = (Object[]) udpMessage.getPayload();
                        String result = market.localValidatePurchase(
                                (String) buyCheckData[0],
                                (String) buyCheckData[1],
                                (int) buyCheckData[2]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "BUY_CHECK", 0, null, result);
                        break;
                    case "PURCHASE":
                        Object[] purchaseData = (Object[]) udpMessage.getPayload();
                        String purchaseResult = market.purchaseShare(
                                (String) purchaseData[0],
                                (String) purchaseData[1],
                                (String) purchaseData[2],
                                (int) purchaseData[3],
                                (String) purchaseData[4]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "PURCHASE", 0, null, purchaseResult);
                        break;
                    case "AVAILABILITY":
                        String shareType = (String) udpMessage.getPayload();
                        String availability = market.getLocalShareAvailability(shareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "AVAILABILITY", 0, null, availability);
                        break;
                    case "DATA-SEND":
                        MarketStateSnapshot snapshotSend = market.getMarketState();
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "DATA", 0, null, snapshotSend);
                        break;
                    case "DATA-RECEIVE":
                        MarketStateSnapshot snapshotReceive = (MarketStateSnapshot) udpMessage.getPayload();
                        market.updateMarketState(snapshotReceive);
                        response = new UDPMessage(UDPMessage.MessageType.ACK, "OK", 0, null, null);
                        break;
                    default:
                        break;
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(response);
                oos.flush();

                byte[] responseBytes = baos.toByteArray();
                DatagramPacket responseData = new DatagramPacket(
                        responseBytes, responseBytes.length, request.getAddress(), request.getPort()
                );
                socket.send(responseData);
            }

        }
        catch(Exception e){
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
}
