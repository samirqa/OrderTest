package ExchangeOrder.utility;

import java.io.IOException;
import java.net.URI;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;


@ClientEndpoint
public class WsClient {
    Session userSession = null;
    private MessageHandler messageHandler;
    private static String endpointURI = ApplicationProperties.getInstance().getProperty("server.endpoint");

    public WsClient(String query) {
        try {
            WebSocketContainer container = ContainerProvider
                    .getWebSocketContainer();
            container.connectToServer(this, new URI(endpointURI+query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close() throws IOException {
    	userSession.close();
    }

    @OnOpen
    public void onOpen(Session userSession) {
        this.userSession = userSession;
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.userSession = null;
    }

    @OnMessage
    public void onMessage(String message) {
        if (this.messageHandler != null)
            this.messageHandler.handleMessage(message);
    }

    public void addMessageHandler(MessageHandler msgHandler) {
        this.messageHandler = msgHandler;
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

    public static interface MessageHandler {
        public void handleMessage(String message);
    }
}
  