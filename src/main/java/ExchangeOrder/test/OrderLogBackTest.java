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

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.ApplicationProperties;
import ExchangeOrder.utility.Log;
import ExchangeOrder.utility.MyKafkaProducer;
import ExchangeOrder.utility.WsClient;
import ExchangeOrder.utility.WsClient.MessageHandler;

public class OrderLogBackTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();
	private List<OrderLogBack> olbList = new ArrayList<OrderLogBack>();
	final ObjectMapper mapper = new ObjectMapper();
	boolean messageReceived = false;
	WsClient cl_orderlogback;
	private int olb_messageCount = 0;
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

		olbList = new ArrayList<OrderLogBack>();
	}

	// Step 2 - pass all parameter empty
	@Test(priority = 1)
	public void Test_EmptyAllInput() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyAllInput");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?consumerId=&memberId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"ConsumerId must be a non empty value!!\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message of Empty member ID is :" + message + " Successfully");
						messageReceived = true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_EmptyAllInput", ExtentColor.GREEN));

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
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyAllInput", ExtentColor.RED));
			throw e;
		}

	}

	// Step 3 - invalid memberid and valid consumer with valid lastLogid
	@Test(priority = 2)
	public void Test_InvalidMemberID() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidMemberID");
			WsClient cl = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=Z&consumerId=TesConsumer&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\r\n" + "  \"error\" : {\r\n" + "    \"code\" : 100,\r\n"
								+ "    \"message\" : \"Member id is not Valid and this connection should be rejected\"\r\n"
								+ "  }\r\n" + "}";

						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message of Invalid member ID is :" + message + " Successfully");
						messageReceived = true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_InvalidMemberID ", ExtentColor.GREEN));

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
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidMemberID", ExtentColor.RED));
			throw e;
		}

	}

	// Step 4 - valid memberid, empty consumerId, and empty orderLogbackid
	@Test(priority = 3)
	public void Test_EmptyConsumerID() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyConsumerID");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\r\n" + "  \"error\" : {\r\n" + "    \"code\" : 100,\r\n"
								+ "    \"message\" : \"ConsumerId must be a non empty value!!\"\r\n" + "  }\r\n" + "}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						messageReceived = true;
						logger.info("Display error message of Empty Conlumer ID is :" + message + " Successfully");
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_EmptyConsumerID ", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Log.info("Wait for error message to be received.");
			Thread.sleep(1000);
			cl.close();
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyConsumerID", ExtentColor.RED));
			throw e;
		}

	}

	// Step 5. Provid valid data, with blank ordlerLogId
	@Test(priority = 4)
	public void Test_ValidData() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_ValidData");
			// logger.log(Status.PASS, MarkupHelper.createLabel("Test_ValidData ",
			// ExtentColor.GREEN));
			WsClient cl = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=A&consumerId=testConsumer&lastOrderLogId=");

			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						LogResult<OrderLogBack> result = mapper.readValue(message,
								new TypeReference<LogResult<OrderLogBack>>() {
								});
						logger.info("Display All messages of valid data :" + message + " Successfully");
						olbList.add(result.getResult());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			// wait for orders to load
			Thread.sleep(2000);
			assertEquals(olbList.size(), 28);
			logger.info("Matched all display data count :" + olbList.size() + "Successfully");
			// assertEquals(olbList, loadExpectedResults());
			cl.close();
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_ValidData ", ExtentColor.GREEN));
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_ValidData", ExtentColor.RED));
			throw e;
		}
	}

	// Step 6. Provider valid memberid and duplicate consumer
	@Test(priority = 5)
	public void Test_DuplicateConsumerID() throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_DuplicateConsumerID");
			WsClient cl = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						LogResult<OrderLogBack> result = mapper.readValue(message,
								new TypeReference<LogResult<OrderLogBack>>() {
								});
						logger.log(Status.INFO, message);
						Log.info(message);
						System.out.println(message);
						olbList.add(result.getResult());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			WsClient cl1 = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
			cl1.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\r\n" + "  \"error\" : {\r\n" + "    \"code\" : 100,\r\n"
								+ "    \"message\" : \"consumer already part of the queue and this connection should be rejected\"\r\n"
								+ "  }\r\n" + "}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display messages of duplicate  consumer ID :" + message + " Successfully");
						messageReceived = true;
						logger.log(Status.PASS,
								MarkupHelper.createLabel("Test_DuplicateConsumerID ", ExtentColor.GREEN));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl1.connect();
			Thread.sleep(1000);
			assertEquals(messageReceived, true);
			cl1.close();
			cl.close();
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_DuplicateConsumerID", ExtentColor.RED));
			throw e;
		}
	}

	// Step 7 - Enter valid Last Log Order ID and Verify that Logs counts must be
	// same as expected for given Kakfa id
	@Test(dataProvider = "TestValidLastOrderLogBackID", priority = 6)

	public void Test_Valid_LastLogOrderID_MsgCount(String memberID, String consumerID, final int kafkastartid,
			final String currentLogbackId, final int expectedOlbMessages) throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_Valid_LastLogOrderID_MsgCount from " + currentLogbackId
					+ "To Goven Kafka ID " + kafkastartid);
			String logbackUrl = "xchange/orderstreaming/orderlogback?memberId=" + memberID + "&consumerId=" + consumerID
					+ "&lastOrderLogId=" + currentLogbackId;
			System.out.println("OrderLogback URL :" + logbackUrl);
			logger.info("OrderLogback URL : " + logbackUrl);
			cl_orderlogback = new WsClient(logbackUrl);

			cl_orderlogback.addMessageHandler(new MessageHandler() {
				public void handleMessage(String message) {
					LogResult<OrderLogBack> result;
					try {
						result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
						});
						// System.out.println("ORDERLOGBACK =" + result.getResult());
						logger.info("ORDERLOGBACK =	" + result.getResult());
						olbList.add(result.getResult());
						olb_messageCount++;
						if (olb_messageCount >= expectedOlbMessages) {
							// System.out.println(result.getResult());
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

			MyKafkaProducer.produceMessage(getOLB(kafkastartid));
			Thread.sleep(100);
			cl_orderlogback.connect();
			// send kafka messages which will be loop back and received on streams.
			Thread t = new Thread(new Runnable() {

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
			t.start();
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
			Thread.sleep(100);
			System.out.println("Orderlogback " + olbList.size());
			logger.info("Total Orderlogback :" + olbList.size());
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
			cl_orderlogback.close();
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_Valid_LastLogOrderID_MsgCount", ExtentColor.GREEN));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_Valid_LastLogOrderID_MsgCount", ExtentColor.RED));
			throw e;
		}
	}

	/**
	 * Here DataProvider returning value on the basis of test method name
	 * 
	 * @param m
	 * @return
	 **/

	@DataProvider(name = "TestValidLastOrderLogBackID")
	public Object[][] getDataFromDataprovider() {
		return new Object[][] { { "A", "Test", 10, "25", 114 }, { "A", "Test", 0, "", 82 },
				// { "A", "Test", 2500, "10",2590 },
		};
	}

	// Step 8 - Enter Invalid TestData and Verify Logs display
	@Test(dataProvider = "InvalidTestData", priority = 7)
	private void Test_InvalidTestData(final String currentLogbackId, final int kafkastartid) throws Exception {
		// olbList = new ArrayList<OrderLogBack>();
		// olbFiledList = new ArrayList<OrderFilledLog>();
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidTestData");
			cl_orderlogback = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer&lastOrderLogId="
							+ currentLogbackId);
			cl_orderlogback.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\n" + "  \"error\" : {\n" + "    \"code\" : 100,\n"
								+ "    \"message\" : \"Log id supplied: " + currentLogbackId + " Cached logId: "
								+ kafkastartid + " less than what the user is requesting for\"\n" + "  }\n" + "}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message for Invalid TestData is :" + message + " Successfully");
						messageReceived = true;
						cl_orderlogback.close();
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_InvalidTestData", ExtentColor.GREEN));
						synchronized (monitor) {
							monitor.notifyAll();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

			MyKafkaProducer.produceMessage(getOLB(kafkastartid));
			cl_orderlogback.connect();

			try {
				synchronized (monitor) {
					monitor.wait();
				}
			} catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			System.out.println("Reached first wait");
			try {
				synchronized (monitor) {
					monitor.wait();
				}
			} catch (IllegalMonitorStateException e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			// TODO: handle exception
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidTestData", ExtentColor.RED));
			throw e;
		}
	}

	@DataProvider(name = "InvalidTestData")
	public Object[][] getInvalidDataFromDataprovider() {
		return new Object[][] {
				// { "26", 25 },
				{ "29", 25 }, };
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
