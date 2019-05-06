package ExchangeOrder.test;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSON;
import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.Log;
import ExchangeOrder.utility.WsClient;
import ExchangeOrder.utility.WsClient.MessageHandler;

public class OrderLogBackTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();
	private List<OrderLogBack> olbList = new ArrayList<OrderLogBack>();
	final ObjectMapper mapper = new ObjectMapper();
	boolean messageReceived = false;

	// Step 2 - pass all parameter empty
	@Test(priority = 1)
	public void Test_EmptyAllInput() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyAllInput");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?consumerId=&memberId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\"error\":{\"code\":100,\"message\":\"ConsumerId must be a non empty value!!\"}}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message of Empty member ID is :"+message+ " Successfully");
						messageReceived=true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_EmptyAllInput", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyAllInput", ExtentColor.RED));
		}

	}

	// Step 3 - invalid memberid and valid consumer with valid lastLogid
	@Test(priority = 2)
	public void Test_InvalidMemberID() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_InvalidMemberID");
			WsClient cl = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=Z&consumerId=TesConsumer&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\r\n" + 
								"  \"error\" : {\r\n" + 
								"    \"code\" : 100,\r\n" + 
								"    \"message\" : \"Member id is not Valid and this connection should be rejected\"\r\n" + 
								"  }\r\n" + 
								"}";
								
						assertEquals(JSON.parse(message), JSON.parse(expected));
						logger.info("Display error message of Invalid member ID is :"+message+ " Successfully");
						messageReceived = true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_InvalidMemberID ", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Thread.sleep(1000);
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_InvalidMemberID", ExtentColor.RED));
		}

	}

	// Step 4 - valid memberid, empty consumerId, and empty orderLogbackid
	@Test(priority = 3)
	public void Test_EmptyConsumerID() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyConsumerID");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						String expected = "{\r\n" + 
								"  \"error\" : {\r\n" + 
								"    \"code\" : 100,\r\n" + 
								"    \"message\" : \"ConsumerId must be a non empty value!!\"\r\n" + 
								"  }\r\n" + 
								"}";
						assertEquals(JSON.parse(message), JSON.parse(expected));
						messageReceived=true;
						logger.info("Display error message of Empty Conlumer ID is :"+message+ " Successfully");
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_EmptyConsumerID ", ExtentColor.GREEN));

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			Log.info("Wait for error message to be received.");
			Thread.sleep(1000);
			assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyConsumerID", ExtentColor.RED));
		}

	}

	// Step 5. Provid valid data, with blank ordlerLogId
	@Test(priority = 4)
	public void Test_ValidData() throws Exception {
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
						LogResult<OrderLogBack> result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
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
			//assertEquals(olbList, loadExpectedResults());
			cl.close();
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_ValidData ", ExtentColor.GREEN));
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_ValidData", ExtentColor.RED));
		}
	}

	// Step 6. Provider valid memberid and duplicate consumer
	@Test(priority = 5)
	public void Test_DuplicateConsumerID() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_DuplicateConsumerID");
			WsClient cl = new WsClient(
					"xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						LogResult<OrderLogBack> result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
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
		
		WsClient cl1 = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
		cl1.addMessageHandler(new MessageHandler() {
			
			public void handleMessage(String message) {
				try {
					String expected = "{\r\n" + 
							"  \"error\" : {\r\n" + 
							"    \"code\" : 100,\r\n" + 
							"    \"message\" : \"consumer already part of the queue and this connection should be rejected\"\r\n" + 
							"  }\r\n" + 
							"}";
					assertEquals(JSON.parse(message), JSON.parse(expected));
					logger.info("Display messages of duplicate  consumer ID :"+ message + " Successfully");
					messageReceived=true;
					logger.log(Status.PASS, MarkupHelper.createLabel("Test_DuplicateConsumerID ", ExtentColor.GREEN));
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		Thread.sleep(1000);
		assertEquals(messageReceived, true);
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_DuplicateConsumerID", ExtentColor.RED));
		}
	}

	// Step 7 - valid memberid, Unique consumerId, and empty orderLogbackid
	@Test(priority = 6)
	public void Test_EmptyLogBackID() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_EmptyLogBackID");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer2&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {

				public void handleMessage(String message) {
					try {
						LogResult<OrderLogBack> result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
						});
						olbList.add(result.getResult());
						logger.info("Display Output :" + message);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			cl.connect();
			// wait for orders to load
			Thread.sleep(2000);

			assertEquals(olbList.size(), 28);
			logger.info("Matched all display data counnt :" + olbList.size() + "Successfully");
			//assertEquals(olbList, loadExpectedResults());
			cl.close();
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_EmptyLogBackID ", ExtentColor.GREEN));
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_EmptyLogBackID", ExtentColor.RED));
		}
	}
	
	// Step 8 - valid memberid, Unique consumerId, and empty orderLogbackid
		@Test(priority = 7)
		public void Test_RecordFromGivenLastOrderID() throws Exception {
			try {
				Log.info("-------Start TestCase" + sTestCaseName + "----------");
				logger = extent.createTest("Test_RecordFromGivenLastOrderID");
				WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer3&lastOrderLogId=10");
				cl.addMessageHandler(new MessageHandler() {

					public void handleMessage(String message) {
						try {
							LogResult<OrderLogBack> result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
							});
							logger.info("Display All messages after lastOrderLogId=10 :" + message + " Successfully");
							olbList.add(result.getResult());
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				// wait for orders to load
				Thread.sleep(2000);

				assertEquals(olbList.size(),18);
				logger.info("Matched all display data count :" + olbList.size() + "Successfully");
				//assertEquals(olbList, loadExpectedResults());
				cl.close();
				logger.log(Status.PASS, MarkupHelper.createLabel("Test_RecordFromGivenLastOrderID ", ExtentColor.GREEN));
			} catch (Exception e) {
				logger.log(Status.FAIL, MarkupHelper.createLabel("Test_RecordFromGivenLastOrderID", ExtentColor.RED));
			}
		}
		// Step 9 - valid memberid, Unique consumerId, and empty orderLogbackid
				@Test(priority = 8)
				public void Test_OutofRangeLastOrderID() throws Exception {
					try {
						Log.info("-------Start TestCase" + sTestCaseName + "----------");
						logger = extent.createTest("Test_OutofRangeLastOrderID");
						WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=29");
						cl.addMessageHandler(new MessageHandler() {

							public void handleMessage(String message) {
								try {
									LogResult<OrderLogBack> result = mapper.readValue(message, new TypeReference<LogResult<OrderLogBack>>() {
									});
									olbList.add(result.getResult());
									logger.info("Display All messages after lastOrderLogId=29 :" + message + " Successfully");
									logger.info("Total size of LogBack :"+olbList.size());
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
						// wait for orders to load

						Thread.sleep(2000);
						assertEquals(olbList.size(),0);
						logger.info("Records featched after lastOrderLogId = 29 are Zero ");
						//assertEquals(olbList, loadExpectedResults());
						cl.close();
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_OutofRangeLastOrderID ", ExtentColor.GREEN));
					} catch (Exception e) {
						logger.log(Status.FAIL, MarkupHelper.createLabel("Test_OutofRangeLastOrderID", ExtentColor.RED));
					}
				}
	private List<OrderLogBack> loadExpectedResults() throws JsonParseException, JsonMappingException, IOException {
		InputStream in = getClass().getClassLoader().getResourceAsStream("ExpectedResult1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<OrderLogBack> olbList = mapper.readValue(in, new TypeReference<List<OrderLogBack>>() {
		});
		return olbList;
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
