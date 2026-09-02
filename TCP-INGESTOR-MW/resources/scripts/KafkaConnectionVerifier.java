// package com.tcp.ingestor.config;

// import org.apache.kafka.clients.admin.AdminClient;
// import org.apache.kafka.clients.admin.ListTopicsOptions;
// import org.springframework.boot.CommandLineRunner;
// import org.springframework.kafka.core.KafkaAdmin;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.stereotype.Component;

// import java.util.Set;

// @Component
// public class KafkaConnectionVerifier implements CommandLineRunner {

//     private final KafkaAdmin kafkaAdmin;
//     private final KafkaTemplate<String, Object> kafkaTemplate;

//     public KafkaConnectionVerifier(KafkaAdmin kafkaAdmin, KafkaTemplate<String, Object> kafkaTemplate) {
//         this.kafkaAdmin = kafkaAdmin;
//         this.kafkaTemplate = kafkaTemplate;
//     }

//     @Override
//     public void run(String... args) {
//         System.out.println("Validating connection to Kafka bootstrap servers...");

//         try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            
//             ListTopicsOptions options = new ListTopicsOptions().timeoutMs(3000);
//             Set<String> topics = client.listTopics(options).names().get();

//             System.out.println("SUCCESSFULLY CONNECTED TO KAFKA!");
//             System.out.println("Existing topics found on cluster: " + topics);

//             // --- TEST MESSAGE EMISSION ---
//             String targetTopic = "log-processing";
//             int targetPartition = 0;
//             String mockJsonPayload = "{\"id\":100,\"level\":\"INFO\",\"message\":\"Spring Boot live connection test payload!\"}";

//             System.out.println("Attempting to send test message to topic '" + targetTopic + "' on Partition " + targetPartition + "...");
            
//             // Sends the message without a key to your specific partition
//             kafkaTemplate.send(targetTopic, targetPartition, null, mockJsonPayload);
            
//             System.out.println("TEST MESSAGE SENT SUCCESSFULLY!");

//         } catch (Exception e) {
//             System.err.println("KAFKA VERIFICATION OR TRANSMISSION FAILED!");
//             System.err.println("Reason: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }
// }
