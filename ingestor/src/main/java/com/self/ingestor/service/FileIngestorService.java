package com.self.ingestor.service;

import com.self.ingestor.model.TradeRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import jakarta.annotation.PreDestroy;

@Service
public class FileIngestorService {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TradeFileParser tradeFileParser;
    private final ExecutorService executorService;

    private final int batchSize;
    private final int threadPoolSize;

    public FileIngestorService(JdbcTemplate jdbcTemplate,
                               KafkaTemplate<String, String> kafkaTemplate,
                               TradeFileParser tradeFileParser,
                               @Value("${ingestor.batch-size:500}") int batchSize,
                               @Value("${ingestor.thread-pool-size:5}") int threadPoolSize) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.tradeFileParser = tradeFileParser;
        this.batchSize = batchSize;
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Transactional
    public void ingestFile(String filePath) throws Exception {
        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        System.out.println("Starting ingestion of trade file: " + filePath);

        if (!Files.exists(path)) {
            throw new RuntimeException("File not found: " + filePath);
        }

        List<Future<Integer>> futures = new ArrayList<>();
        long totalLinesProcessed = 0;
        long totalRecordsProcessed = 0;

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            List<String> batch = new ArrayList<>();
            String line;

            while ((line = reader.readLine()) != null) {
                // Skip empty lines and potential header row
                if (line.trim().isEmpty() || isHeaderRow(line)) {
                    continue;
                }

                batch.add(line);
                totalLinesProcessed++;

                if (batch.size() >= batchSize) {
                    List<String> currentBatch = new ArrayList<>(batch);
                    futures.add(executorService.submit(() ->
                            processTradeBatch(currentBatch, fileName)));
                    batch.clear();
                }
            }

            // Process remaining lines
            if (!batch.isEmpty()) {
                futures.add(executorService.submit(() ->
                        processTradeBatch(batch, fileName)));
            }

            // Wait for all batches to complete
            for (Future<Integer> future : futures) {
                totalRecordsProcessed += future.get();
            }

            System.out.println("Successfully processed " + totalRecordsProcessed + " trade records from " + fileName);

            // Send success event to Kafka
            String successMessage = String.format(
                    "{\"filePath\": \"%s\", \"fileName\": \"%s\", \"type\": \"trade\", \"status\": \"processed\", \"recordsProcessed\": %d}",
                    filePath, fileName, totalRecordsProcessed
            );
            kafkaTemplate.send("file.received", successMessage);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "{\"filePath\": \"%s\", \"fileName\": \"%s\", \"type\": \"trade\", \"status\": \"failed\", \"error\": \"%s\"}",
                    filePath, fileName, e.getMessage()
            );
            kafkaTemplate.send("file.failed", errorMessage);
            throw new RuntimeException("Failed to ingest trade file: " + filePath, e);
        }
    }

    private boolean isHeaderRow(String line) {
        // Simple check for header row (could be enhanced)
        return line.toLowerCase().contains("trade") && line.toLowerCase().contains("amount");
    }

    private int processTradeBatch(List<String> lines, String fileName) {
        try {
            List<TradeRecord> tradeRecords = tradeFileParser.parseFile(lines, fileName);
            return insertTradeRecords(tradeRecords, fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process batch: " + e.getMessage(), e);
        }
    }

    private int insertTradeRecords(List<TradeRecord> tradeRecords, String fileName) {
        String sql = "INSERT INTO trade_staging (trade_id, loan_amount, monthly_interest, tenure_months, total_outstanding, file_name, processed_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, SYSDATE)";

        int[] updateCounts = jdbcTemplate.batchUpdate(sql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                TradeRecord record = tradeRecords.get(i);
                ps.setString(1, record.getTradeId());
                ps.setLong(2, record.getLoanAmount());
                ps.setLong(3, record.getInterest());
                ps.setInt(4, record.getTenureMonths());
                ps.setLong(5, record.getTotalOutstanding());
                ps.setString(6, fileName);
            }

            @Override
            public int getBatchSize() {
                return tradeRecords.size();
            }
        });

        return tradeRecords.size();
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}