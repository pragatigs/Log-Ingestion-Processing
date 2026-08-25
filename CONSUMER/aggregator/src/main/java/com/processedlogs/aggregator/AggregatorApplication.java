package com.processedlogs.aggregator;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.processedlogs.aggregator.cron.ScheduledAggregator;

@SpringBootApplication
public class AggregatorApplication {

	public static void main(String[] args) {
		System.out.println("Starting Aggregator Application...");
		try {
			ScheduledAggregator.main(args);
		} catch (Exception e) {
			throw new RuntimeException("Aggregation failed", e);
		}
	}

}
