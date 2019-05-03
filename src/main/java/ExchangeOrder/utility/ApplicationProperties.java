package ExchangeOrder.utility;

import java.io.IOException;
import java.util.Properties;

public class ApplicationProperties extends Properties {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ApplicationProperties() throws IOException{
		load(getClass().getResourceAsStream("/application.properties"));
	}
	
	private static Boolean symaphore=false;
	
	private static ApplicationProperties _appProps;
	
	public static ApplicationProperties getInstance()
	{
		System.out.println(_appProps);
		synchronized (symaphore) {
			if(_appProps!=null)
				return _appProps;
			else
				try {
					return new ApplicationProperties();
				} catch (IOException e) {
					e.printStackTrace();
				}
			return null;
		}
	}
}
