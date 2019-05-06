package ExchangeOrder.model;

import java.math.BigDecimal;
import java.util.Date;

/**
 * {
 * "matchId": "bbxcsdf",     // 交易处理id
 * "logId": "aaaasdfkljzxc",   // 成交日志id
 * "logCreatedAt": 123456785,    // 成交日志创建时间
 * "symbol": "BTCUSDT",       // 交易对
 * "filledPrice": "0.001",     // 成交价格
 * "filledQuantity": "100",       // 成交数量
 * "filledCreatedAt": 123456785,          //成交时间
 * "bidOrderId": 50,          //  买方的订单ID
 * "askOrderId": 50,          // 卖方的订单ID
 * "maker": true,        // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
 * }
 */
public class OrderFilledLog {
	public OrderFilledLog() {}
    public String matchId;
    public String logId;
    public Date logCreatedAt;


    //交易对代码
    public String symbol;
    //成交的价格
    public BigDecimal filledPrice;
    //成交数量
    public BigDecimal filledQuantity;
    //成交记录创建时间
    public Date filledCreatedAt;


    //买方向的订单id
    public String bidOrderId;
    //卖方向的订单id
    public String askOrderId;
    //买先挂单卖吃单：true，卖先挂单买吃单：false，
    public boolean maker;
    
    public OrderFilledLog(OrderLogBack olb) {
        this.matchId = olb.getMatchId();
        this.logId = olb.getGlobalMatchingEngineLogId().toString();
        this.logCreatedAt = new Date(olb.getLogCreatedAt());
        this.symbol = olb.getSymbol();
        this.filledPrice = olb.getLastFilledPrice();
        this.filledQuantity = olb.getLastFilledQuantity();
        this.filledCreatedAt = new Date(olb.getLastFilledCreatedAt());
        if(this.matchId!=null && !this.matchId.equals("")) {
        	String[] matchSplit = this.matchId.split("-");
        	this.bidOrderId = matchSplit[0].substring(0,matchSplit[0].length()-1);
        	this.askOrderId = matchSplit[1].substring(0,matchSplit[1].length()-1);
        }
        this.maker = (olb.getLastFilledIsTaker() == 1);
    }
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public String getLogId() {
		return logId;
	}
	public void setLogId(String logId) {
		this.logId = logId;
	}
	public Date getLogCreatedAt() {
		return logCreatedAt;
	}
	public void setLogCreatedAt(Date logCreatedAt) {
		this.logCreatedAt = logCreatedAt;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public BigDecimal getFilledPrice() {
		return filledPrice;
	}
	public void setFilledPrice(BigDecimal filledPrice) {
		this.filledPrice = filledPrice;
	}
	public BigDecimal getFilledQuantity() {
		return filledQuantity;
	}
	public void setFilledQuantity(BigDecimal filledQuantity) {
		this.filledQuantity = filledQuantity;
	}
	public Date getFilledCreatedAt() {
		return filledCreatedAt;
	}
	public void setFilledCreatedAt(Date filledCreatedAt) {
		this.filledCreatedAt = filledCreatedAt;
	}
	public String getBidOrderId() {
		return bidOrderId;
	}
	public void setBidOrderId(String bidOrderId) {
		this.bidOrderId = bidOrderId;
	}
	public String getAskOrderId() {
		return askOrderId;
	}
	public void setAskOrderId(String askOrderId) {
		this.askOrderId = askOrderId;
	}
	public boolean isMaker() {
		return maker;
	}
	public void setMaker(boolean maker) {
		this.maker = maker;
	}
	@Override
	public String toString() {
		return "OrderFilledLog [matchId=" + matchId + ", logId=" + logId + ", logCreatedAt=" + logCreatedAt
				+ ", symbol=" + symbol + ", filledPrice=" + filledPrice + ", filledQuantity=" + filledQuantity
				+ ", filledCreatedAt=" + filledCreatedAt + ", bidOrderId=" + bidOrderId + ", askOrderId=" + askOrderId
				+ ", maker=" + maker + "]";
	}
}