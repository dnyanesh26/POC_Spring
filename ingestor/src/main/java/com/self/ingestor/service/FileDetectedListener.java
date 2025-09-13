package com.self.ingestor.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FileDetectedListener {

    private final FileIngestorService fileIngestorService;

    public FileDetectedListener(FileIngestorService fileIngestorService) {
        this.fileIngestorService = fileIngestorService;
    }

    @KafkaListener(topics = "file.detected", groupId = "ingestor-group", containerFactory = "ConcurrentKafkaListenerContainerFactory")
    public void consume(String message) {
        System.out.println("📥 Received from Kafka: " + message);
        try {
            // Simple JSON parsing (could use Jackson)
            String filePath = message.split("\"filePath\":")[1].split(",")[0].replaceAll("[\"{} ]", "");
            System.out.println(filePath);
//            fileIngestorService.ingestFile(filePath);
        } catch (Exception e) {
            System.err.println("❌ Ingestion failed for: " + message + " | " + e.getMessage());
        }
    }
}

