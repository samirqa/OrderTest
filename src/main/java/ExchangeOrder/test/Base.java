package ExchangeOrder.test;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.reporter.ExtentHtmlReporter;
import com.aventstack.extentreports.reporter.configuration.ChartLocation;
import com.aventstack.extentreports.reporter.configuration.Theme;

import ExchangeOrder.utility.ApplicationProperties;
import ExchangeOrder.utility.Constant;

public class Base {

	public static ExtentHtmlReporter htmlReporter;
	public static ExtentReports extent;
	public static ExtentTest logger;
	public static final Logger log4j = Logger.getLogger(Base.class);
	
	@BeforeClass
	//@Parameters(value={"browser"})
	//public void _beforeTest(String browser) {
		public void _beforeTest() {
		try{
		DateFormat df = new SimpleDateFormat("dd.MM.yy-hhmmss");
		File f = new File(ApplicationProperties.getInstance().getProperty("report.dir") + Constant.Path_Report);
		if(!f.exists())
			f.mkdirs();
		
		
		htmlReporter = new ExtentHtmlReporter(ApplicationProperties.getInstance().getProperty("report.dir") + Constant.Path_Report + "OrderbacklogTest_" +df.format(new Date()) + ".html");
	//	String envRootDir = System.getProperty("user.dir");
	//	System.out.println("System Dir  " +envRootDir);
	//	htmlReporter = new ExtentHtmlReporter("E:\\testreports\\TestResources\\TestReports\\OrderbacklogTest.html");
		extent = new ExtentReports();
		extent.attachReporter(htmlReporter);
		extent.setSystemInfo("Host Name", "http://localhost:20003/chat.html");
		extent.setSystemInfo("Environment", "Testing");
		extent.setSystemInfo("Reporter Name", "Samir Patel");

		htmlReporter.config().setDocumentTitle("Exchange Order Test Automation");
		htmlReporter.config().setReportName("Test Execution Report");
		htmlReporter.config().setTestViewChartLocation(ChartLocation.TOP);
		htmlReporter.config().setTheme(Theme.STANDARD);

		if (log4j.isDebugEnabled()) {
			log4j.debug("entering BaseClass BeforeTest()");
		}

		DOMConfigurator.configure("log4j.xml");
	
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public void _afterMethod() {
		try {
		} catch (Exception ignore) {

		}
		if (log4j.isDebugEnabled()) {
			log4j.debug("exiting BaseClass AfterClass()");
		}
	}
}
