package com.example.orders.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/** Customers (enrichment source) and FX rates (fed by the public Frankfurter feed). */
@Repository
public class ReferenceDataRepository {

    public record Customer(String customerId, String name, String tier, BigDecimal creditLimitUsd) {
    }

    public record FxRate(String currency, BigDecimal ratePerUsd, LocalDate asOf, String source) {
    }

    private final NamedParameterJdbcTemplate jdbc;

    public ReferenceDataRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Customer> findCustomer(String customerId) {
        return jdbc.query("SELECT * FROM customers WHERE customer_id = :id",
                new MapSqlParameterSource("id", customerId),
                (rs, i) -> new Customer(rs.getString("customer_id"), rs.getString("name"),
                        rs.getString("tier"), rs.getBigDecimal("credit_limit"))).stream().findFirst();
    }

    public List<Customer> findAllCustomers() {
        return jdbc.query("SELECT * FROM customers ORDER BY customer_id",
                (rs, i) -> new Customer(rs.getString("customer_id"), rs.getString("name"),
                        rs.getString("tier"), rs.getBigDecimal("credit_limit")));
    }

    public Optional<FxRate> findRate(String currency) {
        return jdbc.query("SELECT * FROM fx_rates WHERE currency = :c",
                new MapSqlParameterSource("c", currency), (rs, i) -> toRate(rs)).stream().findFirst();
    }

    public List<FxRate> findAllRates() {
        return jdbc.query("SELECT * FROM fx_rates ORDER BY currency", (rs, i) -> toRate(rs));
    }

    public void upsertRate(String currency, BigDecimal ratePerUsd, LocalDate asOf, String source) {
        jdbc.update("""
                INSERT INTO fx_rates (currency, rate_per_usd, as_of, source, updated_at)
                VALUES (:c, :r, :d, :s, now())
                ON CONFLICT (currency) DO UPDATE
                SET rate_per_usd = EXCLUDED.rate_per_usd, as_of = EXCLUDED.as_of,
                    source = EXCLUDED.source, updated_at = now()
                """, new MapSqlParameterSource()
                .addValue("c", currency)
                .addValue("r", ratePerUsd)
                .addValue("d", Date.valueOf(asOf))
                .addValue("s", source));
    }

    private static FxRate toRate(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new FxRate(rs.getString("currency").trim(), rs.getBigDecimal("rate_per_usd"),
                rs.getDate("as_of").toLocalDate(), rs.getString("source"));
    }
}
