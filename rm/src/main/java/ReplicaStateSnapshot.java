import market.MarketStateSnapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ReplicaStateSnapshot implements Serializable {
    public Map<String, MarketStateSnapshot> marketSnapshots = new HashMap<>();

    public void put(String market, MarketStateSnapshot snapshot){
        marketSnapshots.put(market, snapshot);
    }

    public Map<String, MarketStateSnapshot> getMarketSnapshots(){
        return marketSnapshots;
    }
}
