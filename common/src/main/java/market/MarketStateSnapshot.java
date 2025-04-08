package market;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MarketStateSnapshot implements Serializable {
    private String market;

    private HashMap<String, HashMap<String, Share>> shares; // shareType -> (shareID -> info)
    private HashMap<String, HashMap <String, Integer>> buyerRecords; // buyerID -> (shareID -> # shares)
    private HashMap<String, HashMap<Integer, Integer>> weeklyCrossMarketPurchases; // buyerID -> (week -> count)
    private HashMap<String, HashMap<String, Set<String>>> dailyPurchases;

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }

    public HashMap<String, HashMap<String, Share>> getShares() {
        return shares;
    }

    public void setShares(HashMap<String, HashMap<String, Share>> shares) {
        this.shares = shares;
    }

    public HashMap<String, HashMap<String, Integer>> getBuyerRecords() {
        return buyerRecords;
    }

    public void setBuyerRecords(HashMap<String, HashMap<String, Integer>> buyerRecords) {
        this.buyerRecords = buyerRecords;
    }

    public HashMap<String, HashMap<Integer, Integer>> getWeeklyCrossMarketPurchases() {
        return weeklyCrossMarketPurchases;
    }

    public void setWeeklyCrossMarketPurchases(HashMap<String, HashMap<Integer, Integer>> weeklyCrossMarketPurchases) {
        this.weeklyCrossMarketPurchases = weeklyCrossMarketPurchases;
    }

    public HashMap<String, HashMap<String, Set<String>>> getDailyPurchases() {
        return dailyPurchases;
    }

    public void setDailyPurchases(HashMap<String, HashMap<String, Set<String>>> dailyPurchases) {
        this.dailyPurchases = dailyPurchases;
    }


}

