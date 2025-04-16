import market.MarketImpl;

import javax.xml.ws.Endpoint;
import java.net.InetAddress;

public class TokyoServer {

    Endpoint marketEndpoint;
    UDPServer udpEndpoint;

//    public static void main(String[] args) {
//        MarketImpl market = new MarketImpl();
//        market.initialize("TOK", 1097); // Set market details
//        new UDPServer(1097, market).start();
//        Endpoint.publish("http://localhost:1097/market", market);
//        System.out.println("Tokyo Market (TOK) is live on JAX-WS.");
//    }

    public TokyoServer(InetAddress ip, int port){
        MarketImpl market = new MarketImpl();
        market.initialize("TOK", port, String.valueOf(ip), port-40);
        udpEndpoint = new UDPServer(port, market);
        udpEndpoint.start();
        //String ipPublish = "http://" + ip + ":" + port + "/Tokyo";
        //marketEndpoint = Endpoint.publish(ipPublish, market);
    }
}
