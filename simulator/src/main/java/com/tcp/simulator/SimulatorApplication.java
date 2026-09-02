package com.tcp.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import main.java.com.tcp.simulator.TcpPusher;

@SpringBootApplication
public class SimulatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimulatorApplication.class, args);
		TcpPusher tcpPusher = new TcpPusher();
		tcpPusher.pushTcpLogs();
	}

}
