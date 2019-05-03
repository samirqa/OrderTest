package ExchangeOrder.test;

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;

import ExchangeOrder.utility.Log;

public class OrderLogBackTest extends Base {
	// Getting the Test Case name, as it will going to use in so many places
	private String sTestCaseName = this.toString();

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
