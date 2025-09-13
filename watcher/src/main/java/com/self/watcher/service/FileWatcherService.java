package com.self.watcher.service;


import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.file.*;

@Service
public class FileWatcherService {

    private final KafkaTemplate<String, String> kafkaTemplate;


    @Value("${filewatcher.topic}")
    private String TOPIC;

    @Value("${filewatcher.watch-dir}")
    private String WATCH_DIR;

    public FileWatcherService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;

    }

    @PostConstruct
    public void startWatching() {
        Thread watcherThread = new Thread(this::watchDirectory, "file-watcher-thread");
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    private void watchDirectory() {
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(WATCH_DIR);
            if (!Files.exists(path)) Files.createDirectories(path);

            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            System.out.println("👀 Watching directory: " + WATCH_DIR);

            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                        Path newFile = path.resolve((Path) event.context());
                        String payload = "{ \"filePath\": \"" + newFile + "\", \"timestamp\": \"" + java.time.Instant.now() + "\" }";

                        kafkaTemplate.send(TOPIC, payload);
                        System.out.println("📤 Published to Kafka: " + payload);
                    }
                }
                key.reset();
            }
        } catch (Exception e) {
            throw new RuntimeException("File watcher failed", e);
        }
    }
}

