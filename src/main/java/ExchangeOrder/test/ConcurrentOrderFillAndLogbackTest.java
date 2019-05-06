package ExchangeOrder.test;

import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderFilledLog;
import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.Log;
import ExchangeOrder.utility.MyKafkaProducer;
import ExchangeOrder.utility.WsClient;
import ExchangeOrder.utility.WsClient.MessageHandler;

public class ConcurrentOrderFillAndLogbackTest extends Base {
	private String sTestCaseName = this.toString();
	private List<OrderLogBack> olbList;
	private List<OrderFilledLog> olbFiledList;
	final ObjectMapper mapper = new ObjectMapper();
	boolean messageReceived=false;
	Object monitor = new Object();
	WsClient cl_filledOrderLog;
	WsClient cl_orderlogback;
	
	@Test(priority=1)
	public void Test_ConcurrentTest() throws Exception {
		verifyConcurrentTest("",25,100, 88);
	}
	
	@Test(priority=2)
	public void Test_ConcurrentTest_lastLogIdTest() {
		verifyConcurrentTest("20",25,81, 78);
	}
	
	@Test(priority=3)
	public void Test_ConcurrentTest_underscoreTest() {
		verifyConcurrentTest("_",25,75, 75);
	}
	
	private void verifyConcurrentTest(String currentLogbackId, int kafkastartid, int expectedOlbMessages, int expectedOFLmessages) {
		olbList = new ArrayList<OrderLogBack>();
		olbFiledList = new ArrayList<OrderFilledLog>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ConcurrentTest");
			cl_orderlogback = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="+currentLogbackId);
			cl_filledOrderLog = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId="+currentLogbackId+"&symbol=BTCUSDT");
			cl_orderlogback.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					LogResult<OrderLogBack> result;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
						});
						System.out.println("ORDERLOGBACK"+result.getResult());
						olbList.add(result.getResult());
						if(result.getResult().getGlobalMatchingEngineLogId().compareTo(BigInteger.valueOf(100))>=0) {
							System.out.println(result.getResult());
							try {
								cl_orderlogback.close();
							}catch (Exception e) {
								e.printStackTrace();
							}
							synchronized (monitor) {
								monitor.notifyAll();								
							}
						}						
					} catch (JsonParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonMappingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			
			cl_filledOrderLog.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					LogResult<OrderFilledLog> result=null;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderFilledLog>>() {
						});
						System.out.println("ORDERFILLEDLOG"+result.getResult());
						olbFiledList.add(result.getResult());
						if(Long.valueOf(result.getResult().getLogId())>=100) {
							System.out.println(result.getResult());
							try {
							cl_filledOrderLog.close();
							}catch (Exception e) {
								e.printStackTrace();
							}
							synchronized (monitor) {
								monitor.notifyAll();								
							}
						}
					} catch (JsonParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonMappingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			MyKafkaProducer.produceMessage(getOLB(kafkastartid));
			Thread.sleep(100);
			cl_filledOrderLog.connect();
			cl_orderlogback.connect();
			
			//send kafka messages which will be loop back and received on streams.
			for(int i=kafkastartid+1;i<=100;i++) {
				MyKafkaProducer.produceMessage(getOLB(i));
			}
			
			try {
			synchronized (monitor) {
				monitor.wait();
			}
			}catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			try {
			synchronized (monitor) {
				monitor.wait();
			}
			}catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			
			System.out.println("Orderlogback "+olbList.size());
			System.out.println("OrderfilledLog "+olbFiledList.size());
			assertTrue(olbFiledList.size() == expectedOFLmessages, expectedOFLmessages + " messages not received for order filled log");
			assertTrue(olbList.size() == expectedOlbMessages,expectedOlbMessages + " messages not received for order log back");
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}		
	}
	
	private LogResult<OrderLogBack> getOLB(int logId) {
		OrderLogBack olb = new OrderLogBack();
		olb.setOrderId(UUID.randomUUID().toString());
		olb.setOrderSide("BUY");
		olb.setOrderType("LIMIT");
		olb.setGlobalMatchingEngineLogId(BigInteger.valueOf(logId));
		olb.setMemberId("A");
		olb.setDealtCurrency("BTC");
		olb.setQuoteCurrency("USDT");
		olb.setCreatedAt(new Date().getTime());
		olb.setUpdatedAt(new Date().getTime());
		olb.setCancelledAt(new Date().getTime());
		olb.setLogCreatedAt(new Date().getTime());
		olb.setQuantity(new BigDecimal("0.0012"));
		olb.setLimitPrice(new BigDecimal("5329.0034"));
		olb.setOpenQuantity(new BigDecimal("10"));
		olb.setFilledCumulativeQuantity(new BigDecimal("0.7"));
		olb.setLastFilledPrice(new BigDecimal("5340.34"));
		olb.setLastFilledQuantity(new BigDecimal("0.0045"));
		olb.setLastFilledCreatedAt(new Date().getTime());
		olb.setLastFilledIsTaker(Short.valueOf("1"));
		olb.setMatchId("BTCUSDT:7BUY5001abcgggffffffff11a-BTCUSDT:9SELL5000abcgggffffffff11a");
		olb.setMatchingEngineLogId(BigInteger.valueOf(logId));
		olb.setOrderStatus("FILLED");

		olb.setMatchingEngineId(10);
		LogResult<OrderLogBack> res = new LogResult<OrderLogBack>();
		res.setResult(olb);
		return res;
	}
}
