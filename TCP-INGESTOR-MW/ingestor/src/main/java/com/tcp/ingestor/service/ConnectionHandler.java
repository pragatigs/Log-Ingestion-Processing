package com.tcp.ingestor.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import org.springframework.stereotype.Service;

@Service
public class ConnectionHandler {
    private final LogRouterService logRouterService;
    private final KafkaProducerService kafkaProducerService;
    
    public ConnectionHandler(LogRouterService logRouterService, KafkaProducerService kafkaProducerService){
        this.logRouterService = logRouterService;
        this.kafkaProducerService = kafkaProducerService;
    }

    public void handle(Socket socket){
            try {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

            String data;

            while ((data = reader.readLine()) != null) {
                System.out.println("Received: " + data);
            
            int partition = logRouterService.decidePartition(data);
            kafkaProducerService.sendMessage("log-processing", partition, data);
            }

    }
    catch (IOException e){
        e.printStackTrace();
    }
}
}
