package main.java.com.tcp.simulator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class TcpPusher {

    public void pushTcpLogs() {

        try (Socket socket = new Socket("localhost", 5001);
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            writer.println("{\"id\":1,\"level\":\"INFO\",\"message\":\"Payment received\"}");
            writer.println("{\"id\":2,\"level\":\"DEBUG\",\"message\":\"Processing payment\"}");
            writer.println("{\"id\":3,\"level\":\"WARN\",\"message\":\"Payment processing is slow\"}");
            writer.println("{\"id\":4,\"level\":\"ERROR\",\"message\":\"Payment failed\"}");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
