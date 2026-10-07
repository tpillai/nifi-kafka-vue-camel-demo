package com.example.orders.persistence;

import com.example.orders.model.Order;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderRepository {

    private static final RowMapper<Order> ROW_MAPPER = (rs, i) -> {
        Order o = new Order();
        o.setOrderId(rs.getString("order_id"));
        o.setCustomerId(rs.getString("customer_id"));
        o.setCustomerName(rs.getString("customer_name"));
        o.setCustomerTier(rs.getString("customer_tier"));
        o.setProduct(rs.getString("product"));
        o.setQuantity(rs.getInt("quantity"));
        o.setAmount(rs.getBigDecimal("amount"));
        o.setCurrency(rs.getString("currency"));
        o.setAmountUsd(rs.getBigDecimal("amount_usd"));
        o.setFxRate(rs.getBigDecimal("fx_rate"));
        o.setPriority(rs.getString("priority"));
        o.setStatus(rs.getString("status"));
        o.setLane(rs.getString("lane"));
        o.setReason(rs.getString("reason"));
        o.setSource(rs.getString("source"));
        o.setSubmittedBy(rs.getString("submitted_by"));
        o.setReceivedAt(rs.getTimestamp("received_at").toInstant().toString());
        o.setProcessedAt(rs.getTimestamp("processed_at").toInstant().toString());
        return o;
    };

    private final NamedParameterJdbcTemplate jdbc;

    public OrderRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the processed order. ON CONFLICT is the second line of defence behind the
     * idempotent consumer: a redelivered orderId never creates a second row.
     */
    public Order save(Order o) {
        Instant now = Instant.now();
        o.setProcessedAt(now.toString());
        jdbc.update("""
                INSERT INTO orders (order_id, customer_id, customer_name, customer_tier, product, quantity,
                                    amount, currency, amount_usd, fx_rate, priority, status, lane, reason,
                                    source, submitted_by, received_at, processed_at)
                VALUES (:orderId, :customerId, :customerName, :customerTier, :product, :quantity,
                        :amount, :currency, :amountUsd, :fxRate, :priority, :status, :lane, :reason,
                        :source, :submittedBy, :receivedAt, :processedAt)
                ON CONFLICT (order_id) DO NOTHING
                """, new MapSqlParameterSource()
                .addValue("orderId", UUID.fromString(o.getOrderId()))
                .addValue("customerId", o.getCustomerId())
                .addValue("customerName", o.getCustomerName())
                .addValue("customerTier", o.getCustomerTier())
                .addValue("product", o.getProduct())
                .addValue("quantity", o.getQuantity())
                .addValue("amount", o.getAmount())
                .addValue("currency", o.getCurrency())
                .addValue("amountUsd", o.getAmountUsd())
                .addValue("fxRate", o.getFxRate())
                .addValue("priority", o.getPriority())
                .addValue("status", o.getStatus())
                .addValue("lane", o.getLane())
                .addValue("reason", o.getReason())
                .addValue("source", o.getSource())
                .addValue("submittedBy", o.getSubmittedBy())
                .addValue("receivedAt", Timestamp.from(OffsetDateTime.parse(o.getReceivedAt()).toInstant()))
                .addValue("processedAt", Timestamp.from(now)));
        return o;
    }

    public Optional<Order> findById(UUID orderId) {
        return jdbc.query("SELECT * FROM orders WHERE order_id = :id",
                new MapSqlParameterSource("id", orderId), ROW_MAPPER).stream().findFirst();
    }

    public List<Order> find(String status, String customerId, int limit) {
        return jdbc.query("""
                SELECT * FROM orders
                WHERE (CAST(:status AS VARCHAR) IS NULL OR status = :status)
                  AND (CAST(:customerId AS VARCHAR) IS NULL OR customer_id = :customerId)
                ORDER BY processed_at DESC
                LIMIT :limit
                """, new MapSqlParameterSource()
                .addValue("status", status)
                .addValue("customerId", customerId)
                .addValue("limit", limit), ROW_MAPPER);
    }
}
