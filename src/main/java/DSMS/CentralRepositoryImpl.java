package DSMS;

import javax.jws.WebService;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@WebService(endpointInterface = "DSMS.CentralRepository")
public class CentralRepositoryImpl implements CentralRepository {


    private final Map<String, Integer> marketServers;
    private final Map<String, String> shareRegistry;

    public CentralRepositoryImpl(){
        marketServers = new HashMap<>();
        shareRegistry = new HashMap<>();
    }

    private synchronized void log(String message){
        String path = "logs/CentralRepository.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            writer.write(message);
            writer.newLine();
        }
        catch (IOException e){
            System.err.println("Error writing to log file: " + path);
            e.printStackTrace();
        }
    }

    /**
     * Registers the given market into the central repository.
     * This maps each market to a port, allowing dynamic access for clients.
     *
     * @param market    The Market being stored (NYK, LON, or TOK)
     * @param host      The host for the server. In this case, always localhost.
     * @param port      The port for the given sever.
     */
    @Override
    public synchronized void registerMarketServer(String market, String host, int port){
        marketServers.put(market, port);
        log(LocalDateTime.now() + "- Registered Market " + market + " to port " + port);
    }

    /**
     * Returns the whole hashmap serving markets and ports
     *
     * @return  The hashmap containing all server info
     */
    @Override
    public synchronized String[] getMarketServers() {
        return marketServers.keySet().toArray(new String[0]);
    }

    /**
     * Registers a given share into the central repository.
     * This enables the clients to query the central repository and know where to connect.
     *
     * @param shareID   The share being added
     * @param market    The market associated with the share
     */
    @Override
    public synchronized void registerShare(String shareID, String market){
        shareRegistry.put(shareID, market);
        log(LocalDateTime.now() + "- Registered Share " + shareID + " to Market " + market);
    }

    /**
     * Returns the market to which a share belongs.
     *
     * @param shareID   The share ID being queried
     * @return          The market string associated with the share
     */
    @Override
    public synchronized String getMarketForShare(String shareID){
        return shareRegistry.getOrDefault(shareID, "NONE");
    }
}
