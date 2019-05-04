package ExchangeOrder.model;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown=true)
public class OrderLogBack  {
	private long index_id;
	private String memberId;
	private String orderId;
	private String orderStatus;
	private long createdAt;
	private long updatedAt;
	private long cancelledAt;
	private String matchId;
	private long logCreatedAt;
	private String dealtCurrency;
	private String quoteCurrency;
	private String orderType;
	private String orderSide;
	private BigDecimal quantity;
	private BigDecimal limitPrice;
	private BigDecimal openQuantity;
	private BigDecimal filledCumulativeQuantity;
	private BigDecimal lastFilledPrice;
	private BigDecimal lastFilledQuantity;
	private long lastFilledCreatedAt;
	private short lastFilledIsTaker;
	private String linkedHash;
	
	@JsonInclude(Include.NON_NULL)
	private Short routable;
	
	@JsonInclude(Include.NON_NULL)
	private String lastFilledVenue;
	
	@JsonInclude(Include.NON_NULL)
	private String lastFilledSymbol;
	
	@JsonInclude(Include.NON_NULL)
	private Integer matchingEngineId; // small int
	private BigInteger matchingEngineLogId;   // bigInt
	private BigInteger globalMatchingEngineLogId; // bigint
	private String partitionId;
	private long partitionOffset;
	
	public BigInteger getGlobalMatchingEngineLogId() {
		return globalMatchingEngineLogId;
	}
	public void setGlobalMatchingEngineLogId(BigInteger globalMatchingEngineLogId) {
		this.globalMatchingEngineLogId = globalMatchingEngineLogId;
	}
	public BigInteger getMatchingEngineLogId() {
		return matchingEngineLogId;
	}
	public void setMatchingEngineLogId(BigInteger matchingEngineLogId) {
		this.matchingEngineLogId = matchingEngineLogId;
	}
	public String getSymbol() {
		return this.getDealtCurrency()+this.getQuoteCurrency();
	}
	public long getIndex_id() {
		return index_id;
	}
	public void setIndex_id(long index_id) {
		this.index_id = index_id;
	}
	public String getMemberId() {
		return memberId;
	}
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getOrderStatus() {
		return orderStatus;
	}
	public void setOrderStatus(String orderStatus) {
		this.orderStatus = orderStatus;
	}
	public long getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}
	public long getUpdatedAt() {
		return updatedAt;
	}
	public void setUpdatedAt(long updatedAt) {
		this.updatedAt = updatedAt;
	}
	public long getCancelledAt() {
		return cancelledAt;
	}
	public void setCancelledAt(long cancelledAt) {
		this.cancelledAt = cancelledAt;
	}
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public long getLogCreatedAt() {
		return logCreatedAt;
	}
	public void setLogCreatedAt(long logCreatedAt) {
		this.logCreatedAt = logCreatedAt;
	}
	public String getDealtCurrency() {
		return dealtCurrency;
	}
	public void setDealtCurrency(String dealtCurrency) {
		this.dealtCurrency = dealtCurrency;
	}
	public String getQuoteCurrency() {
		return quoteCurrency;
	}
	public void setQuoteCurrency(String quoteCurrency) {
		this.quoteCurrency = quoteCurrency;
	}
	public String getOrderType() {
		return orderType;
	}
	public void setOrderType(String orderType) {
		this.orderType = orderType;
	}
	public String getOrderSide() {
		return orderSide;
	}
	public void setOrderSide(String orderSide) {
		this.orderSide = orderSide;
	}
	public BigDecimal getQuantity() {
		return quantity;
	}
	public void setQuantity(BigDecimal quantity) {
		this.quantity = quantity;
	}
	public BigDecimal getLimitPrice() {
		return limitPrice;
	}
	public void setLimitPrice(BigDecimal limitPrice) {
		this.limitPrice = limitPrice;
	}
	public BigDecimal getOpenQuantity() {
		return openQuantity;
	}
	public void setOpenQuantity(BigDecimal openQuantity) {
		this.openQuantity = openQuantity;
	}
	public BigDecimal getFilledCumulativeQuantity() {
		return filledCumulativeQuantity;
	}
	public void setFilledCumulativeQuantity(BigDecimal filledCumulativeQuantity) {
		this.filledCumulativeQuantity = filledCumulativeQuantity;
	}
	public BigDecimal getLastFilledPrice() {
		return lastFilledPrice;
	}
	public void setLastFilledPrice(BigDecimal lastFilledPrice) {
		this.lastFilledPrice = lastFilledPrice;
	}
	public BigDecimal getLastFilledQuantity() {
		return lastFilledQuantity;
	}
	public void setLastFilledQuantity(BigDecimal lastFilledQuantity) {
		this.lastFilledQuantity = lastFilledQuantity;
	}
	public long getLastFilledCreatedAt() {
		return lastFilledCreatedAt;
	}
	public void setLastFilledCreatedAt(long lastFilledCreatedAt) {
		this.lastFilledCreatedAt = lastFilledCreatedAt;
	}
	public short getLastFilledIsTaker() {
		return lastFilledIsTaker;
	}
	public void setLastFilledIsTaker(short lastFilledIsTaker) {
		this.lastFilledIsTaker = lastFilledIsTaker;
	}
	public String getLinkedHash() {
		return linkedHash;
	}
	public void setLinkedHash(String linkedHash) {
		this.linkedHash = linkedHash;
	}
	public Short getRoutable() {
		return routable;
	}
	public void setRoutable(Short routable) {
		this.routable = routable;
	}
	public String getLastFilledVenue() {
		return lastFilledVenue;
	}
	public void setLastFilledVenue(String lastFilledVenue) {
		this.lastFilledVenue = lastFilledVenue;
	}
	public String getLastFilledSymbol() {
		return lastFilledSymbol;
	}
	public void setLastFilledSymbol(String lastFilledSymbol) {
		this.lastFilledSymbol = lastFilledSymbol;
	}
	public Integer getMatchingEngineId() {
		return matchingEngineId;
	}
	public void setMatchingEngineId(Integer matchingEngineId) {
		this.matchingEngineId = matchingEngineId;
	}
	public String getPartitionId() {
		return partitionId;
	}
	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}
	public long getPartitionOffset() {
		return partitionOffset;
	}
	public void setPartitionOffset(long partitionOffset) {
		this.partitionOffset = partitionOffset;
	}
	
	@Override
	public String toString() {
		return "OrderLogBack [index_id=" + index_id + ", memberId=" + memberId + ", orderId=" + orderId
				+ ", orderStatus=" + orderStatus + ", createdAt=" + createdAt + ", updatedAt=" + updatedAt
				+ ", cancelledAt=" + cancelledAt + ", matchId=" + matchId + ", logCreatedAt=" + logCreatedAt
				+ ", dealtCurrency=" + dealtCurrency + ", quoteCurrency=" + quoteCurrency + ", orderType=" + orderType
				+ ", orderSide=" + orderSide + ", quantity=" + quantity + ", limitPrice=" + limitPrice
				+ ", openQuantity=" + openQuantity + ", filledCumulativeQuantity=" + filledCumulativeQuantity
				+ ", lastFilledPrice=" + lastFilledPrice + ", lastFilledQuantity=" + lastFilledQuantity
				+ ", lastFilledCreatedAt=" + lastFilledCreatedAt + ", lastFilledIsTaker=" + lastFilledIsTaker
				+ ", linkedHash=" + linkedHash + ", routable=" + routable + ", lastFilledVenue=" + lastFilledVenue
				+ ", lastFilledSymbol=" + lastFilledSymbol + ", matchingEngineId=" + matchingEngineId
				+ ", matchingEngineLogId=" + matchingEngineLogId + ", globalMatchingEngineLogId="
				+ globalMatchingEngineLogId + ", partitionId=" + partitionId + ", partitionOffset=" + partitionOffset
				+ "]";
	}
	
		
}
