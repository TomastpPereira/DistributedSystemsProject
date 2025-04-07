import market.MarketStateSnapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ReplicaStateSnapshot implements Serializable {
    public Map<String, MarketStateSnapshot> marketSnapshots = new HashMap<>();
}
