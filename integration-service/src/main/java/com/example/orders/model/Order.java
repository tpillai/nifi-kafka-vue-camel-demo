package com.example.orders.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;

/**
 * The order message as it travels through Kafka.
 * Inbound (orders.raw) it carries what the gateway or a partner file supplied;
 * Camel fills in the enrichment and decision fields before publishing to orders.processed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Order {

    // Supplied by the source
    private String orderId;
    private String customerId;
    private String product;
    private Integer quantity;
    private BigDecimal amount;
    private String currency;
    private String priority;
    private String source;
    private String submittedBy;
    private String receivedAt;

    // Added by Camel
    private String customerName;
    private String customerTier;
    private BigDecimal fxRate;
    private BigDecimal amountUsd;
    private String status;
    private String lane;
    private String reason;
    private String processedAt;

    @JsonIgnore
    private BigDecimal creditLimitUsd;

    public void approve(String lane) {
        this.status = "APPROVED";
        this.lane = lane;
    }

    public void reject(String reason) {
        this.status = "REJECTED";
        this.lane = "NONE";
        this.reason = reason;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public String getSubmittedBy() { return submittedBy; }
    public void setSubmittedBy(String submittedBy) { this.submittedBy = submittedBy; }
    public String getReceivedAt() { return receivedAt; }
    public void setReceivedAt(String receivedAt) { this.receivedAt = receivedAt; }
    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }
    public String getCustomerTier() { return customerTier; }
    public void setCustomerTier(String customerTier) { this.customerTier = customerTier; }
    public BigDecimal getFxRate() { return fxRate; }
    public void setFxRate(BigDecimal fxRate) { this.fxRate = fxRate; }
    public BigDecimal getAmountUsd() { return amountUsd; }
    public void setAmountUsd(BigDecimal amountUsd) { this.amountUsd = amountUsd; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getLane() { return lane; }
    public void setLane(String lane) { this.lane = lane; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public String getProcessedAt() { return processedAt; }
    public void setProcessedAt(String processedAt) { this.processedAt = processedAt; }
    public BigDecimal getCreditLimitUsd() { return creditLimitUsd; }
    public void setCreditLimitUsd(BigDecimal creditLimitUsd) { this.creditLimitUsd = creditLimitUsd; }
}
