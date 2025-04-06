package market;

import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface CentralRepository {

    void registerMarketServer(String market, String host, int port);

    String getMarketForShare(String shareID);

    void registerShare(String shareID, String market);

    String[] getMarketServers();
}
