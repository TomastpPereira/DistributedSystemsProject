package market;

import market.Share;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MarketStateSnapshot implements Serializable {
    private String market;
    private Map<String, Map<String, Share>> shares; // shareType -> (shareID -> info)
    private Map<String, Map <String, Integer>> buyerRecords; // buyerID -> (shareID -> # shares)
    private Map<String, Map<Integer, Integer>> weeklyCrossMarketPurchases; // buyerID -> (week -> count)
    private Map<String, Map<String, Set<String>>> dailyPurchases;

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }

    public Map<String, Map<String, Share>> getShares() {
        return shares;
    }

    public void setShares(Map<String, Map<String, Share>> shares) {
        this.shares = shares;
    }

    public Map<String, Map<String, Integer>> getBuyerRecords() {
        return buyerRecords;
    }

    public void setBuyerRecords(Map<String, Map<String, Integer>> buyerRecords) {
        this.buyerRecords = buyerRecords;
    }

    public Map<String, Map<Integer, Integer>> getWeeklyCrossMarketPurchases() {
        return weeklyCrossMarketPurchases;
    }

    public void setWeeklyCrossMarketPurchases(Map<String, Map<Integer, Integer>> weeklyCrossMarketPurchases) {
        this.weeklyCrossMarketPurchases = weeklyCrossMarketPurchases;
    }

    public Map<String, Map<String, Set<String>>> getDailyPurchases() {
        return dailyPurchases;
    }

    public void setDailyPurchases(Map<String, Map<String, Set<String>>> dailyPurchases) {
        this.dailyPurchases = dailyPurchases;
    }

}

