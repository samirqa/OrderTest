package ExchangeOrder.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderFilledLog;
import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.ApplicationProperties;
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
	AdminClient admin;
	private int olb_messageCount;
	private int ofl_messageCount;
	
	@BeforeMethod
	public void setup() throws InterruptedException {
		System.out.println(ApplicationProperties.getInstance().get("kafka.brockers"));
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ApplicationProperties.getInstance().get("kafka.brockers"));
		
		admin = AdminClient.create(config);
		admin.deleteTopics(getTopicArray());
		
		admin.createTopics(getNewToppics());
		Thread.sleep(1000); // wait is added as created topic need some time to be cached in the cluster.
		olb_messageCount=0;
		ofl_messageCount=0;
		olbList = new ArrayList<OrderLogBack>();
		olbFiledList = new ArrayList<OrderFilledLog>();
	}
	
	private Collection<String> getTopicArray() {
		return Arrays.asList(ApplicationProperties.getInstance().get("kafka.publisher.topic").toString().split(","));
	}
	private Collection<NewTopic> getNewToppics() {
		ArrayList<NewTopic> topics = new ArrayList<NewTopic>();
		Map<String, String> configs = new HashMap<String, String>();
		int partitions = 1;
		int replication = 1;
		topics.add(new NewTopic(ApplicationProperties.getInstance().get("kafka.publisher.topic").toString(),partitions,(short) replication ).configs(configs));
		return topics;
	}
	
	@Test(priority=0) 
	public void Test_kafkaProducer() throws JsonProcessingException, InterruptedException, ExecutionException {
		System.out.println(getTopicArray());
		DescribeTopicsResult results = admin.describeTopics(getTopicArray());
		ObjectMapper mapper = new ObjectMapper();
		System.out.println(mapper.writeValueAsString(results));
		Thread.sleep(1000);
		MyKafkaProducer.produceMessage(getOLB(10));
		results = admin.describeTopics(getTopicArray());
		System.out.println(mapper.writeValueAsString(results));
	}

	
	@Test(priority=1)
	public void Test_ConcurrentTest() throws Exception {
		verifyConcurrentTest("",25,124, 112);
	}
	
	@Test(priority=2)
	public void Test_ConcurrentTest_lastLogIdTest() {
		verifyConcurrentTest("20",25,105, 102);
	}
	
	@Test(priority=3)
	public void Test_ConcurrentTest_underscoreTest() {
		verifyConcurrentTest("_",25,99, 99);
	}
	
	@Test(priority=4)
	public void Test_ConcurrentTest_invalidTest() {
		verifyInvalidTest("26", 25);
	}
	
	@Test(priority=5)
	public void Test_ConcurrentTest_boundrycase() {
		verifyConcurrentTest("25",25,100, 100);
	}
	
	@Test(priority=6)
	public void Test_ConcurrentTest_bigNumber() {
		verifyConcurrentTest("3500",3600,200, 200);
	}
	
	
	@Test(priority=7)
	public void Test_ConcurrentTest_bigNumber_largeGap() {
		verifyConcurrentTest("2000",3600,1700, 1700);
	}
	
	private void verifyInvalidTest(final String currentLogbackId, final int kafkastartid) {
		olbList = new ArrayList<OrderLogBack>();
		olbFiledList = new ArrayList<OrderFilledLog>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ConcurrentTest");
			cl_orderlogback = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="+currentLogbackId);
			cl_filledOrderLog = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId="+currentLogbackId+"&symbol=BTCUSDT");
			cl_orderlogback.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					String expected = "{\n" + 
							"  \"error\" : {\n" + 
							"    \"code\" : 100,\n" + 
							"    \"message\" : \"Log id supplied: "+currentLogbackId+" Cached logId: "+kafkastartid+" less than what the user is requesting for\"\n" + 
							"  }\n" + 
							"}";
					assertEquals(JSON.parse(message), JSON.parse(expected));
					try {
						cl_orderlogback.close();
						synchronized (monitor) {
							monitor.notifyAll();								
						}
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
			cl_filledOrderLog.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					String expected = "{\n" + 
							"  \"error\" : {\n" + 
							"    \"code\" : 100,\n" + 
							"    \"message\" : \"Log id supplied: "+currentLogbackId+" Cached logId: "+kafkastartid+" less than what the user is requesting for\"\n" + 
							"  }\n" + 
							"}";
					assertEquals(JSON.parse(message), JSON.parse(expected));
					try {
						cl_orderlogback.close();
						synchronized (monitor) {
							monitor.notifyAll();								
						}
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			MyKafkaProducer.produceMessage(getOLB(kafkastartid));
			
			cl_filledOrderLog.connect();
			cl_orderlogback.connect();
			
			try {
			synchronized (monitor) {
				monitor.wait();
			}
			}catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			System.out.println("Reached first wait");
			try {
			synchronized (monitor) {
				monitor.wait();
			}
			}catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	private void verifyConcurrentTest(String currentLogbackId, final int kafkastartid, final int expectedOlbMessages,final int expectedOFLmessages) {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ConcurrentTest");
			String logbackUrl = "xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="+currentLogbackId;
			String orderFilledUrl = "xchange/orderstreaming/orderfilled?lastOrderLogId="+currentLogbackId+"&symbol=BTCUSDT";
			System.out.println("Logback"+logbackUrl);
			System.out.println("OrderFilled"+orderFilledUrl);
			cl_orderlogback = new WsClient(logbackUrl);
			cl_filledOrderLog = new WsClient(orderFilledUrl);
			cl_orderlogback.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					LogResult<OrderLogBack> result;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
						});
						System.out.println("ORDERLOGBACK"+result.getResult());
						olbList.add(result.getResult());
						olb_messageCount++;
						if(olb_messageCount>=expectedOlbMessages) {
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
						ofl_messageCount++;
						if(ofl_messageCount>=expectedOFLmessages) {
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
			Thread t1 = new Thread(new Runnable() {
				
				public void run() {
					// TODO Auto-generated method stub
					for(int i=kafkastartid+1;i<=kafkastartid+99;i++) {
						try {
							System.out.println("Producing logid "+i);
							MyKafkaProducer.produceMessage(getOLB(i));
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}					
				}
			});
			t1.start();
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
			System.out.println("wait for some more messages");
			Thread.sleep(2000);
			System.out.println("Orderlogback "+olbList.size());
			System.out.println("OrderfilledLog "+olbFiledList.size());
			assertTrue(olbFiledList.size() == expectedOFLmessages, expectedOFLmessages + " messages not received for order filled log");
			assertTrue(olbList.size() == expectedOlbMessages,expectedOlbMessages + " messages not received for order log back");
			if(!(currentLogbackId.equals("_") || currentLogbackId.equals("")))
			{
				System.out.println("First log is "+olbList.get(0).getGlobalMatchingEngineLogId()+" current"+new BigInteger(currentLogbackId));
				assertTrue(olbList.get(0).getGlobalMatchingEngineLogId().compareTo(new BigInteger(currentLogbackId))==0, "Frist order log should be "+currentLogbackId );
			}
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
