package DSMS;

import javax.xml.ws.Endpoint;

public class NYServer {

    public static void main(String[] args) {
        MarketImpl market = new MarketImpl();
        market.initialize("NYK", 1098); // Set market details
        new UDPServer(1098, market).start();
        Endpoint.publish("http://localhost:1098/market", market);
        System.out.println("New York Market (NYK) is live on JAX-WS.");
    }
}
