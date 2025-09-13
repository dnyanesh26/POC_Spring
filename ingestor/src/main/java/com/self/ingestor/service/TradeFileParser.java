package com.self.ingestor.service;

import com.self.ingestor.model.TradeRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Service
public class TradeFileParser {

    private static final String PIPE_DELIMITER = "\\|";
    private static final int EXPECTED_COLUMNS = 5;
    private static final Pattern TRADE_ID_PATTERN = Pattern.compile("^[A-Z0-9]{8}$");

    public List<TradeRecord> parseFile(List<String> lines, String fileName) throws Exception {
        List<TradeRecord> tradeRecords = new ArrayList<>();
        int lineNumber = 0;

        for (String line : lines) {
            lineNumber++;

            // Skip empty lines
            if (line.trim().isEmpty()) {
                continue;
            }

            try {
                TradeRecord record = parseLine(line, lineNumber, fileName);
                tradeRecords.add(record);
            } catch (Exception e) {
                throw new Exception("Error parsing line " + lineNumber + " in file " + fileName + ": " + e.getMessage());
            }
        }

        return tradeRecords;
    }

    private TradeRecord parseLine(String line, int lineNumber, String fileName) throws Exception {
        String[] columns = line.split(PIPE_DELIMITER, -1); // -1 to include trailing empty strings

        // Validate column count
        if (columns.length != EXPECTED_COLUMNS) {
            throw new Exception("Expected " + EXPECTED_COLUMNS + " columns but found " + columns.length);
        }

        TradeRecord record = new TradeRecord();

        // Parse and validate each column
        record.setTradeId(validateAndParseTradeId(columns[0].trim(), lineNumber));
        record.setLoanAmount(validateAndParseLong("Loan Amount", columns[1].trim(), lineNumber));
        record.setInterest(validateAndParseLong("Monthly Interest", columns[2].trim(), lineNumber));
        record.setTenureMonths(validateAndParseInt("Tenure Months", columns[3].trim(), lineNumber));
        record.setTotalOutstanding(validateAndParseLong("Total Outstanding", columns[4].trim(), lineNumber));

        // Additional business validation
        validateBusinessRules(record, lineNumber);

        return record;
    }

    private String validateAndParseTradeId(String value, int lineNumber) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            throw new Exception("Trade ID cannot be empty");
        }

        if (value.length() != 8) {
            throw new Exception("Trade ID must be exactly 8 characters long");
        }

        if (!TRADE_ID_PATTERN.matcher(value).matches()) {
            throw new Exception("Trade ID must contain only uppercase letters and numbers");
        }

        return value;
    }

    private long validateAndParseLong(String fieldName, String value, int lineNumber) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            throw new Exception(fieldName + " cannot be empty");
        }

        try {
            long number = Long.parseLong(value);
            if (number < 0) {
                throw new Exception(fieldName + " cannot be negative");
            }
            return number;
        } catch (NumberFormatException e) {
            throw new Exception(fieldName + " must be a valid whole number");
        }
    }

    private int validateAndParseInt(String fieldName, String value, int lineNumber) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            throw new Exception(fieldName + " cannot be empty");
        }

        try {
            int number = Integer.parseInt(value);
            if (number <= 0) {
                throw new Exception(fieldName + " must be positive");
            }
            return number;
        } catch (NumberFormatException e) {
            throw new Exception(fieldName + " must be a valid whole number");
        }
    }

    private void validateBusinessRules(TradeRecord record, int lineNumber) throws Exception {
        // Validate that total outstanding >= loan amount
        if (record.getTotalOutstanding() < record.getLoanAmount()) {
            throw new Exception("Total outstanding amount cannot be less than loan amount");
        }

        // Validate that interest is reasonable (e.g., not more than 50% of loan amount)
        if (record.getInterest() > record.getLoanAmount() * 0.5) {
            throw new Exception("Monthly interest seems unusually high");
        }

        // Validate tenure is reasonable (e.g., between 1-360 months)
        if (record.getTenureMonths() < 1 || record.getTenureMonths() > 360) {
            throw new Exception("Tenure must be between 1 and 360 months");
        }
    }
}