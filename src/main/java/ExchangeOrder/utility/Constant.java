package ExchangeOrder.utility;

public class Constant {
	public static final String Path_Report = "//TestReports//";

    public static String KAFKA_BROKERS = ApplicationProperties.getInstance().getProperty("kafka.brockers");

    public static String TOPIC_NAME=ApplicationProperties.getInstance().getProperty("kafka.publisher.topic");

    public static String GROUP_ID_CONFIG=ApplicationProperties.getInstance().getProperty("kafka.group-id");

    public static String OFFSET_RESET=ApplicationProperties.getInstance().getProperty("auto-offset-reset");

}
