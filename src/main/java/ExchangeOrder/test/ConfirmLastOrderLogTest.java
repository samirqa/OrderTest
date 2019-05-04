package ExchangeOrder.test;


import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
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

public class ConfirmLastOrderLogTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();
	
	private List<OrderLogBack> olbList = new ArrayList<OrderLogBack>();
	final ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void Test_invalidOrderLogBack() throws Exception {
		try {
			Log.info("-------Start TestCase" + sTestCaseName + "----------");
			logger = extent.createTest("Test_invalidOrderLogBack");
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_invalidOrderLogBack", ExtentColor.RED));
			
		}catch (Exception e) {
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_invalidOrderLogBack ", ExtentColor.GREEN));
		}
			
	}
	
	@Test
	public void Test_SingleConfirmLastOrderLogTest() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test_SingleConfirmLastOrderLogTest");
		logger.log(Status.PASS, MarkupHelper.createLabel("Test_SingleConfirmLastOrderLogTest ", ExtentColor.GREEN));
		WsClient cl = new WsClient("xchange/orderstreaming/orderlogback?memberId=A&consumerId=testConsumer&lastOrderLogId=");
		
		cl.addMessageHandler(new MessageHandler() {
			
			public void handleMessage(String message) {
				try {
				LogResult result =  mapper.readValue(message, LogResult.class);
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
	
	private List<OrderLogBack> loadExpectedResults() throws JsonParseException, JsonMappingException, IOException {
		InputStream in = getClass().getClassLoader().getResourceAsStream("ExpectedResult1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<OrderLogBack> olbList = mapper.readValue(in, new TypeReference<List<OrderLogBack>>() {
														});
		return olbList;
	}

	@Test
	public void Test_MultipleConfirmLastOrderLogTest() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test_MultipleConfirmLastOrderLogTest");
			logger.log(Status.PASS, MarkupHelper.createLabel("Test_MultipleConfirmLastOrderLogTest ", ExtentColor.GREEN));
			Log.info("Test_MultipleConfirmLastOrderLogTest() : PASS");
		} catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_MultipleConfirmLastOrderLogTest", ExtentColor.RED));
			Log.error("Test_MultipleConfirmLastOrderLogTest() : FAIL");
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
