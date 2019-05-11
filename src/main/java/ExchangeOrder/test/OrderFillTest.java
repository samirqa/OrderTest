package ExchangeOrder.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSON;
import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.media.jfxmedia.logging.Logger;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderFilledLog;
import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.ApplicationProperties;
import ExchangeOrder.utility.Log;
import ExchangeOrder.utility.MyKafkaProducer;
import ExchangeOrder.utility.WsClient;
import ExchangeOrder.utility.WsClient.MessageHandler;

public class OrderFillTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();
	private List<OrderLogBack> olbList;
	private List<OrderFilledLog> olbFiledList;
	final ObjectMapper mapper = new ObjectMapper();
	boolean messageReceived = false;
	private int olb_messageCount;
	private int ofl_messageCount;
	WsClient cl_filledOrderLog;
	WsClient cl_orderlogback;
	Object monitor = new Object();

	@BeforeMethod
	public void setup() throws InterruptedException {
		System.out.println(ApplicationProperties.getInstance().get("kafka.brockers"));
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				ApplicationProperties.getInstance().get("kafka.brockers"));

		// admin = AdminClient.create(config);
		// admin.deleteTopics(getTopicArray());

		// admin.createTopics(getNewToppics());
		// Thread.sleep(1000); // wait is added as created topic need some time to be
		// cached in the cluster.
		olb_messageCount = 0;
		ofl_messageCount = 0;
		olbList = new ArrayList<OrderLogBack>();
		olbFiledList = new ArrayList<OrderFilledLog>();
	}

	// Step 2 - pass all parameter empty
	@Test(priority = 1)
	public void Test_EmptyAllOrderFillInput() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyAllOrderFillInput");
			WsClient cl = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId=&symbol=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"Please provide a valid symbol!!\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message of Empty Input :" + message + " Successfully");
						messageReceived = true;
						Log.info(message);
						logger.log(Status.PASS,
								MarkupHelper.createLabel("Test_EmptyAllOrderFillInput", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			cl.close();
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyAllOrderFillInput", ExtentColor.RED));
			throw e;
		}
	}

	// Step 3 - Enter Invalid Symbol
	@Test(priority = 2)
	public void Test_InvalidSymbol() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidSymbol");
			WsClient cl = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId=&symbol=test");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"Please provide a valid symbol!!\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message Invalid Symbol is :" + message + " Successfully");
						messageReceived = true;
						Log.info(message);
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_InvalidSymbol", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			cl.close();
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidSymbol", ExtentColor.RED));
			throw e;
		}

	}

	// Step 4 - Enter valid Symbol in lower case (ex. btcusdt)
	@Test(priority = 3)
	public void Test_ValidSymbolLowerCase() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ValidSymbolLowerCase");
			WsClient cl = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId=&symbol=btcusdt");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"Please provide a valid symbol!!\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message Invalid Symbol is :" + message + " Successfully");
						messageReceived = true;
						Log.info(message);
						logger.log(Status.PASS,
								MarkupHelper.createLabel("Test_ValidSymbolLowerCase", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			cl.close();
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_ValidSymbolLowerCase", ExtentColor.RED));
			throw e;
		}

	}

	// Step 5 - Enter Invalid lastOrderLogId
	@Test(priority = 4)
	public void Test_InvalidLastLogID() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidLastLogID");
			WsClient cl = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId=test&symbol=btcusdt");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"Log id supplied is not valid !! 0\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message Invalid Symbol is :" + message + " Successfully");
						messageReceived = true;
						Log.info(message);
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_InvalidLastLogID", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			cl.close();
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidLastLogID", ExtentColor.RED));
			throw e;
		}

	}
	// Step 6 - Enter Invalid TestData and Verify Logs display 
	@Test(dataProvider = "InvalidTestData", priority = 5)
	private void Test_InvalidTestData(final String currentLogbackId, final int kafkastartid) throws Exception {
		//olbList = new ArrayList<OrderLogBack>();
		//olbFiledList = new ArrayList<OrderFilledLog>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidTestData");
			cl_orderlogback = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="+currentLogbackId);
			cl_filledOrderLog = new WsClient("xchange/orderstreaming/orderfilled?lastOrderLogId="+currentLogbackId+"&symbol=BTCUSDT");
			cl_orderlogback.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					try {
					String expected = "{\n" + 
							"  \"error\" : {\n" + 
							"    \"code\" : 100,\n" + 
							"    \"message\" : \"Log id supplied: "+currentLogbackId+" Cached logId: "+kafkastartid+" less than what the user is requesting for\"\n" + 
							"  }\n" + 
							"}";
					assertEquals(JSON.parse(message), JSON.parse(expected));
					logger.info("Display error message for Invalid TestData is :" + message + " Successfully");
					messageReceived = true;
					cl_orderlogback.close();
					logger.log(Status.PASS,
							MarkupHelper.createLabel("Test_InvalidTestData", ExtentColor.GREEN));
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
					try {
					String expected = "{\n" + 
							"  \"error\" : {\n" + 
							"    \"code\" : 100,\n" + 
							"    \"message\" : \"Log id supplied: "+currentLogbackId+" Cached logId: "+kafkastartid+" less than what the user is requesting for\"\n" + 
							"  }\n" + 
							"}";
					assertEquals(JSON.parse(message), JSON.parse(expected));
					logger.info("Display error message Invalid TestData is :" + message + " Successfully");
					messageReceived = true;
					
						cl_orderlogback.close();
						logger.log(Status.PASS,
								MarkupHelper.createLabel("Test_InvalidTestData", ExtentColor.GREEN));
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
			assertEquals(messageReceived, true);
		}
		catch (Exception e) {
			// TODO: handle exception
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidTestData", ExtentColor.RED));
			throw e;
		}
	}
	// Step 7 - Enter valid Symbol and Verify Logs display (ex. BTCUSDT)
	@Test(dataProvider = "TestDataForBoundrycase", priority = 6)

	public void Test_ValidInput_And_ValidSymbol(String currentLogbackId, final int kafkastartid,
			final int expectedOlbMessages, final int expectedOFLmessages, String Symbol) throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ValidInput_And_ValidSymbol from "+currentLogbackId +"&"+kafkastartid);
			String logbackUrl = "xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="
					+ currentLogbackId;
			String orderFilledUrl = "xchange/orderstreaming/orderfilled?lastOrderLogId=" + currentLogbackId + "&symbol="
					+ Symbol;
			System.out.println("Logback URL :" + logbackUrl);
			logger.info("Logback URL : " + logbackUrl);
			System.out.println("OrderFilled URL :" + orderFilledUrl);
			logger.info("OrderFilled URL :" + orderFilledUrl);
			cl_orderlogback = new WsClient(logbackUrl);
			cl_filledOrderLog = new WsClient(orderFilledUrl);
			
			cl_orderlogback.addMessageHandler(new MessageHandler() {
				public void handleMessage(String message) {
					LogResult<OrderLogBack> result;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
						});
						//System.out.println("ORDERLOGBACK =" + result.getResult());
						logger.info("ORDERLOGBACK =	" + result.getResult());
						olbList.add(result.getResult());
						olb_messageCount++;
						if (olb_messageCount >= expectedOlbMessages) {
							//System.out.println(result.getResult());
							logger.info("ORDERLOGBACK" + result.getResult());
							try {
								cl_orderlogback.close();
							} catch (Exception e) {
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
					LogResult<OrderFilledLog> result = null;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderFilledLog>>() {
						});
						//System.out.println("ORDERFILLEDLOG =" + result.getResult());
						logger.info("ORDERFILLEDLOG" + result.getResult());
						olbFiledList.add(result.getResult());
						ofl_messageCount++;
						if (ofl_messageCount >= expectedOFLmessages) {
							System.out.println(result.getResult());
							logger.info("ORDERFILLEDLOG" + result.getResult());
							try {
								cl_filledOrderLog.close();
							} catch (Exception e) {
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
			// send kafka messages which will be loop back and received on streams.
			Thread t1 = new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					for (int i = kafkastartid + 1; i <= kafkastartid + 99; i++) {
						try {
							System.out.println("Producing logid " + i);
							logger.info("Producing logid : " + i);
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
			} catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			try {
				synchronized (monitor) {
					monitor.wait();
				}
			} catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			System.out.println("wait for some more messages");
			Thread.sleep(2000);
			System.out.println("Orderlogback " + olbList.size());
			logger.info("Total Orderlogback :" + olbList.size());
			System.out.println("OrderfilledLog " + olbFiledList.size());
			logger.info("Total OrderfilledLog : " + olbFiledList.size());
			assertTrue(olbFiledList.size() == expectedOFLmessages,
					expectedOFLmessages + " messages not received for order filled log");
			assertTrue(olbList.size() == expectedOlbMessages,
					expectedOlbMessages + " messages not received for order log back");
			if (!(currentLogbackId.equals("_") || currentLogbackId.equals(""))) {
				System.out.println("First log is " + olbList.get(0).getGlobalMatchingEngineLogId() + " current"
						+ new BigInteger(currentLogbackId));
				logger.info("First log is " + olbList.get(0).getGlobalMatchingEngineLogId() + " current"
						+ new BigInteger(currentLogbackId));
				assertTrue(
						olbList.get(0).getGlobalMatchingEngineLogId().compareTo(new BigInteger(currentLogbackId)) == 0,
						"Frist order log should be " + currentLogbackId);
			}
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_ValidInput_And_ValidSymbol", ExtentColor.GREEN));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_ValidInput_And_ValidSymbol", ExtentColor.RED));
			throw e;
		}
	}

	/**
	 * Here DataProvider returning value on the basis of test method name
	 * 
	 * @param m
	 * @return
	 **/

	@DataProvider(name = "TestDataForBoundrycase")
	public Object[][] getDataFromDataprovider() {
		return new Object[][] {
			{ "20", 25, 105, 102, "BTCUSDT" },
		//	{ "_", 25, 99, 99, "BTCUSDT" },
		//	{ "3500", 3600, 200, 200, "BTCUSDT" }, 
			};
	}
	@DataProvider(name = "InvalidTestData")
	public Object[][] getInvalidDataFromDataprovider() {
		return new Object[][] {
			//{ "26", 25 },
			{ "30",30},
			};
	}

	private List<OrderLogBack> loadExpectedResults() throws JsonParseException, JsonMappingException, IOException {
		InputStream in = getClass().getClassLoader().getResourceAsStream("ExpectedResult1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<OrderLogBack> olbList = mapper.readValue(in, new TypeReference<List<OrderLogBack>>() {
		});
		return olbList;
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

	@AfterMethod
	public void getResult(ITestResult result) throws Exception {
		Log.endTestCase(sTestCaseName);
		if (result.getStatus() == ITestResult.FAILURE) {
			// logger.log(Status.FAIL, "Test Case Failed is "+result.getName());
			// MarkupHelper is used to display the output in different colors
			logger.log(Status.FAIL,
					MarkupHelper.createLabel(result.getName() + " - Test Case Failed", ExtentColor.RED));
			logger.log(Status.FAIL,
					MarkupHelper.createLabel(result.getThrowable() + " - Test Case Failed", ExtentColor.RED));
		} else if (result.getStatus() == ITestResult.SKIP) {
			// logger.log(Status.SKIP, "Test Case Skipped is "+result.getName());
			logger.log(Status.SKIP,
					MarkupHelper.createLabel(result.getName() + " - Test Case Skipped", ExtentColor.ORANGE));
		}
	}

	@AfterTest
	public void endReport() {
		extent.flush();
	}
}
