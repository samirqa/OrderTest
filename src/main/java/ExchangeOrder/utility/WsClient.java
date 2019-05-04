package ExchangeOrder.utility;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderLogBack;

@ClientEndpoint
public class WsClient {
    private static String server_url = ApplicationProperties.getInstance().getProperty("server.endpoint");
    private static ObjectMapper mapper = new ObjectMapper();
    public static List<OrderLogBack> logs;

    @OnMessage
    public void onMessage(String message) throws JsonParseException, JsonMappingException, IOException {
       System.out.println("Received msg: "+message);
       LogResult res = mapper.readValue(message, LogResult.class);
       logs.add(res.getResult());
    }
    
    public static void connectToSocket(String query) throws DeploymentException, IOException {
        WebSocketContainer container=null;
        Session session=null;
    	container = ContainerProvider.getWebSocketContainer();
    	session = container.connectToServer(WsClient.class, URI.create(server_url+query));
    	logs = new ArrayList<OrderLogBack>();
    }
}
