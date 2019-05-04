package ExchangeOrder.test;


import static org.testng.Assert.assertEquals;

import java.util.Iterator;

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;

import ExchangeOrder.model.OrderLogBack;
import ExchangeOrder.utility.Log;
import ExchangeOrder.utility.WsClient;

public class ConfirmLastOrderLogTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();

	@Test
	public void Test_SingleConfirmLastOrderLogTest() throws Exception {
		try {
		Log.info("-------Start TestCase" + sTestCaseName + "----------");
		logger = extent.createTest("Test_SingleConfirmLastOrderLogTest");
		logger.log(Status.PASS, MarkupHelper.createLabel("Test_SingleConfirmLastOrderLogTest ", ExtentColor.GREEN));
		WsClient.connectToSocket("xchange/orderstreaming/orderlogback?memberId=A&consumerId=testConsumer&lastOrderLogId=");
		
		//wait for orders to load
		Thread.sleep(2000);
		
		Iterator<OrderLogBack> itr = WsClient.logs.iterator();
		OrderLogBack olb;
		assertEquals(WsClient.logs.size(), 28);
		while(itr.hasNext()) {
			olb = itr.next();
			Log.info("OLB : "+olb);
		}
		Log.info("Test_SingleConfirmLastOrderLogTest() : PASS");
		}catch (Exception e) {
			logger.log(Status.FAIL, MarkupHelper.createLabel("Test_SingleConfirmLastOrderLogTest", ExtentColor.RED));
			Log.error("Test_OrderbackLogValidation() : FAIL");
			Log.error(e.getMessage());
			throw (e);
		}
		
		
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
