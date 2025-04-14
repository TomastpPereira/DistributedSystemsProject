import market.MarketImpl;

import javax.xml.ws.Endpoint;
import java.net.InetAddress;

public class NYServer {

    Endpoint marketEndpoint;
    UDPServer udpEndpoint;

//    public static void main(String[] args) {
//        MarketImpl market = new MarketImpl();
//        market.initialize("NYK", 1098); // Set market details
//        new UDPServer(1098, market).start();
//        Endpoint.publish("http://localhost:1098/market", market);
//        System.out.println("New York Market (NYK) is live on JAX-WS.");
//    }

    public NYServer(InetAddress ip, int port){
        MarketImpl market = new MarketImpl();
        market.initialize("NY", port, String.valueOf(ip), port-30);
        udpEndpoint = new UDPServer(port, market);
        udpEndpoint.start();
        String ipPublish = "http://" + ip + ":" + port + "/NY";
        marketEndpoint = Endpoint.publish(ipPublish, market);
    }
}
