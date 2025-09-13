package com.self.ingestor.model;


import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "trade_staging")
public class TradeRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trade_id", length = 8, nullable = false)
    private String tradeId;

    @Column(name = "loan_amount", nullable = false)
    private Long loanAmount;

    @Column(name = "interest", nullable = false)
    private Long interest;

    @Column(name = "tenure_months", nullable = false)
    private Integer tenureMonths;

    @Column(name = "total_outstanding", nullable = false)
    private Long totalOutstanding;

    @Column(name = "file_name", nullable = false)
    private String fileName;

    @Column(name = "processed_date")
    private LocalDateTime processedDate;

    // Constructors, getters, setters
    public TradeRecord() {}

    public TradeRecord(TradeRecord record, String fileName) {
        this.tradeId = record.getTradeId();
        this.loanAmount = record.getLoanAmount();
        this.interest = record.getInterest();
        this.tenureMonths = record.getTenureMonths();
        this.totalOutstanding = record.getTotalOutstanding();
        this.fileName = fileName;
        this.processedDate = LocalDateTime.now();
    }



    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTradeId() {
        return tradeId;
    }

    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public Long getLoanAmount() {
        return loanAmount;
    }

    public void setLoanAmount(Long loanAmount) {
        this.loanAmount = loanAmount;
    }

    public Long getInterest() {
        return interest;
    }

    public void setInterest(Long interest) {
        this.interest = interest;
    }

    public Integer getTenureMonths() {
        return tenureMonths;
    }

    public void setTenureMonths(Integer tenureMonths) {
        this.tenureMonths = tenureMonths;
    }

    public Long getTotalOutstanding() {
        return totalOutstanding;
    }

    public void setTotalOutstanding(Long totalOutstanding) {
        this.totalOutstanding = totalOutstanding;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public LocalDateTime getProcessedDate() {
        return processedDate;
    }

    public void setProcessedDate(LocalDateTime processedDate) {
        this.processedDate = processedDate;
    }
}
