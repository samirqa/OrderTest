package ExchangeOrder.test;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
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
	public String reportPath;
	@BeforeClass
	//@Parameters(value={"browser"})
		public void _beforeTest() {
		try{
		DateFormat df = new SimpleDateFormat("dd.MM.yy-hhmmss");
		File f = new File(System.getProperty("user.dir") + Constant.Path_Report);
		System.out.println("File path : "+f);
		if(!f.exists())
			f.mkdirs();
		reportPath = f + "\\OrderbacklogTest_" +df.format(new Date()) + ".html";
		System.out.println("Report path "+reportPath);
		htmlReporter = new ExtentHtmlReporter(reportPath);
		extent = new ExtentReports();
		extent.attachReporter(htmlReporter);
		extent.setSystemInfo("Host Name", "http://localhost:20003/chat.html");
		extent.setSystemInfo("Environment", "Testing");
		extent.setSystemInfo("Reporter Name", "Samir Patel");
		extent.setSystemInfo("Report Name",reportPath);

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
	@AfterTest
	public void _tearDown() throws IOException {
		String current = new java.io.File( "." ).getCanonicalPath();
		System.out.println("Current path :"+current);
		System.out.println(reportPath);
		File f = new File(reportPath);
		Desktop.getDesktop().open(f);
	}
	@AfterClass
	public void _afterMethod() {
		try {
//			String current = new java.io.File( "." ).getCanonicalPath();
//			System.out.println(reportPath);
//			File f = new File(reportPath);
//			Desktop.getDesktop().open(f);
		} catch (Exception ignore) {

		}
		if (log4j.isDebugEnabled()) {
			log4j.debug("exiting BaseClass AfterClass()");
		}
	}
}
