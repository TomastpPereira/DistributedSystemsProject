package DSMS;

import javax.xml.ws.Endpoint;

public class LondonServer {

    public static void main(String[] args) {
        MarketImpl market = new MarketImpl();
        market.initialize("LON", 1099); // Set market details
        new UDPServer(1099, market).start();
        Endpoint.publish("http://localhost:1099/market", market);
        System.out.println("London Market (LON) is live on JAX-WS.");
    }

}
