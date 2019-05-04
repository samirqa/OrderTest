package ExchangeOrder.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

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
	boolean messageReceived=false;
	//Step 2 - pass all parameter empty  
	@Test
	public void Test_invalidOrderLogBack_AllInvalid() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_invalidOrderLogBack_AllInvalid");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=&consumerId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					try {
						assertEquals(message, "{\n" + 
								"  \"error\" : {\n" + 
								"    \"code\" : 100,\n" + 
								"    \"message\" : \"ConsumerId must be a non empty value!!\"\n" + 
								"  }\n" + 
								"}");
						logger.info(message);
						messageReceived=true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_invalidOrderLogBack_AllInvalid", ExtentColor.GREEN));
						
					}catch (Exception e) {
					}
				}
			});
			
			Thread.sleep(1000);
			assertEquals(messageReceived,true);
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_invalidOrderLogBack", ExtentColor.RED));
		}
			
	}

	//Step 3 - invalid memberid and valid consumer with valid lastLogid  
	@Test
	public void Test_invalidOrderLogBack_InvalidMedmberID() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_invalidOrderLogBack");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=Z&consumerId=TesConsumer&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					try {
						assertEquals(message, "{\n" + 
								"  \"error\" : {\n" + 
								"    \"code\" : 100,\n" + 
								"    \"message\" : \"Member id is not Valid and this connection should be rejected\"\n" + 
								"  }\n" + 
								"}");
						logger.info(message);
						messageReceived = true;
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_invalidOrderLogBack_InvalidMedmberID ", ExtentColor.GREEN));
						
					}catch (Exception e) {
					}
				}
			});
			
			Thread.sleep(1000);
			assertEquals(messageReceived,true);
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_invalidOrderLogBack", ExtentColor.RED));
		}
			
	}

	//Step 4 - valid memberid, empty consumerId, and empty orderLogbackid  
	@Test
	public void Test_invalidOrderLogBack_AllValid() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_invalidOrderLogBack");
			WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=&lastOrderLogId=");
			cl.addMessageHandler(new MessageHandler() {
				
				public void handleMessage(String message) {
					try {
						assertEquals(message, "{\n" + 
								"  \"error\" : {\n" + 
								"    \"code\" : 100,\n" + 
								"    \"message\" : \"ConsumerId must be a non empty value!!\"\n" + 
								"  }\n" + 
								"}");
						messageReceived=true;
						logger.info(message);
						logger.log(Status.PASS, MarkupHelper.createLabel("Test_invalidOrderLogBack_InvalidMedmberID ", ExtentColor.GREEN));
						
					}catch (Exception e) {
					}
				}
			});
		

			Thread.sleep(1000);
			assertEquals(messageReceived,true);
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_invalidOrderLogBack", ExtentColor.RED));
		}
			
	}

	//Step 5. Provid valid data, with blanck ordlerLogId
	@Test
	public void Test_SingleConfirmLastOrderLog_ValidData() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test_SingleConfirmLastOrderLogTest");
		logger.log(Status.PASS, MarkupHelper.createLabel("Test_SingleConfirmLastOrderLogTest ", ExtentColor.GREEN));
		WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=testConsumer&lastOrderLogId=");
		
		cl.addMessageHandler(new MessageHandler() {
			
			public void handleMessage(String message) {
				try {
				LogResult result =  mapper.readValue(message, LogResult.class);
				logger.log(Status.PASS, message);
				Log.info(message);
				System.out.println(message);
				olbList.add(result.getResult());
				}catch (Exception e) {
				}
			}
		});
		//wait for orders to load
		Thread.sleep(2000);
		
		assertEquals(olbList.size(), 28);
		assertEquals(olbList, loadExpectedResults());
		cl.close();
		Log.info("Test_SingleConfirmLastOrderLogTest() : PASS");
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_SingleConfirmLastOrderLogTest", ExtentColor.RED));
			Log.error("Test_OrderbackLogValidation() : FAIL");
			Log.error(e.getMessage());
			throw (e);
		}		
	}
	
	//Step 6. Provider valid memberid and duplicate consumer
	@Test
	public void Test_SingleConfirmLastOrderLog_DuplicateConsumer() throws Exception {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test_invalidOrderLogBack");
		WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
		cl.addMessageHandler(new MessageHandler() {
			
			public void handleMessage(String message) {
				try {
				LogResult result =  mapper.readValue(message, LogResult.class);
				logger.log(Status.PASS, message);
				Log.info(message);
				System.out.println(message);
				olbList.add(result.getResult());
				}catch (Exception e) {
				}
			}
		});
		
		WsClient cl1 = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=TestConsumer1&lastOrderLogId=_");
		cl1.addMessageHandler(new MessageHandler() {
			
			public void handleMessage(String message) {
				try {
					assertEquals(message, "{\n" + 
							"  \"error\" : {\n" + 
							"    \"code\" : 100,\n" + 
							"    \"message\" : \"consumer already part of the queue and this connection should be rejected\"\n" + 
							"  }\n" + 
							"}");
					logger.info(message);
					messageReceived=true;
					logger.log(Status.PASS, MarkupHelper.createLabel("Test_invalidOrderLogBack_InvalidMedmberID ", ExtentColor.GREEN));
				}catch (Exception e) {
				}
			}
		});
		
		Thread.sleep(1000);
		assertEquals(messageReceived,true);
	}
	
	

	private List<OrderLogBack> loadExpectedResults() throws JsonParseException, JsonMappingException, IOException {
		InputStream in = getClass().getClassLoader().getResourceAsStream("ExpectedResult1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<OrderLogBack> olbList = mapper.readValue(in, new TypeReference<List<OrderLogBack>>() {
														});
		return olbList;
	}

	@Test
	public void Test_OrderbackLogValidation() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test OrderaBackLogValidation");
		logger.log(Status.PASS, MarkupHelper.createLabel("Test_OrderbackLogValidation ", ExtentColor.GREEN));
		Log.info("Test_OrderbackLogValidation() : PASS");
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_OrderbackLogValidation", ExtentColor.RED));
			Log.error("Test_OrderbackLogValidation() : FAIL");
			Log.error(e.getMessage());
			throw (e);
		}
	}

	@Test
	public void Test_OrderbackLogInvalidInput() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test InvalidInput OrderBackLog");
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_OrderbackLogInvalidInput ", ExtentColor.GREEN));
			Log.info("Test_OrderbackLogInvalidInput() : PASS");
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_OrderbackLogInvalidInput", ExtentColor.RED));
			Log.error("Test_OrderbackLogInvalidInput() : FAIL");
			Log.error(e.getMessage());
			throw (e);
		}
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
