package com.tcp.ingestor.service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import org.springframework.core.task.TaskExecutor;

@Service
public class TcpServerService implements SmartLifecycle{
    final Logger logger = LogManager.getLogger(TcpServerService.class);
    private ServerSocket serverSocket;
    private final TaskExecutor taskExecutor;
    private final ConnectionHandler connectionHandler;

    public TcpServerService (TaskExecutor taskExecutor, ConnectionHandler connectionHandler){
        this.taskExecutor = taskExecutor;
        this.connectionHandler = connectionHandler;
    }

    @Override
    public void start(){
        try{
            serverSocket =new ServerSocket(5001);
            logger.info("TCP Server started on port 5001");
            taskExecutor.execute(this::acceptConnections);
        }
        catch (IOException e){
            logger.error("Failed to establish connection ", e);
        }
    }

    private void acceptConnections(){
        while (isRunning()){
            try{
                Socket socket = serverSocket.accept();
                logger.info("Connection accepted");
                taskExecutor.execute(()->connectionHandler.handle(socket));
            }
            catch (IOException e){
                if (isRunning()){
                    logger.error("Error accepting TCP connection ",e);
                }
            }
        }
    }

    @Override
    public void stop(){
        try{
            if (serverSocket!=null && !serverSocket.isClosed()){
                logger.info("Stopping TCP connection");
                serverSocket.close();
                logger.info("TCP connection stopped");
            }
        }
        catch (IOException e){
            logger.error("Connection failed to close ",e);
        }
    }

    @Override
    public boolean isRunning(){
        return serverSocket!=null && !serverSocket.isClosed();
    }
    
}
