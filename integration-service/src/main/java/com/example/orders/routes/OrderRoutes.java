package com.example.orders.routes;

import com.example.orders.model.InvalidOrderException;
import com.example.orders.model.Order;
import com.example.orders.model.UnknownCustomerException;
import com.example.orders.persistence.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.idempotent.jdbc.JdbcMessageIdRepository;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Enterprise Integration Patterns used here:
 * idempotent consumer, content enricher, content-based router, wire tap, dead letter channel.
 */
@Component
public class OrderRoutes extends RouteBuilder {

    private final OrderProcessor processor;
    private final OrderRepository orders;
    private final DataSource dataSource;

    public OrderRoutes(OrderProcessor processor, OrderRepository orders, DataSource dataSource) {
        this.processor = processor;
        this.orders = orders;
        this.dataSource = dataSource;
    }

    @Override
    public void configure() {
        // Transient failures (database down, broker hiccup): retry with backoff, then park in the DLT.
        errorHandler(deadLetterChannel("direct:dead-letter")
                .useOriginalMessage()
                .maximumRedeliveries(3)
                .redeliveryDelay(1000)
                .useExponentialBackOff()
                .backOffMultiplier(2)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // Poison or business-invalid messages: retrying cannot fix them, so go straight to the DLT.
        onException(JsonProcessingException.class, InvalidOrderException.class, UnknownCustomerException.class)
                .handled(true)
                .maximumRedeliveries(0)
                .useOriginalMessage()
                .to("direct:dead-letter");

        from("kafka:{{orders.topic.raw}}?groupId=integration-service&autoOffsetReset=earliest")
                .routeId("order-intake")
                .log("Received order key=${header[kafka.KEY]} partition=${header[kafka.PARTITION]} offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, Order.class)
                .bean(processor, "validate")
                // Idempotent consumer: Kafka is at-least-once, so the same orderId can arrive twice.
                .idempotentConsumer(simple("${body.orderId}"),
                        new JdbcMessageIdRepository(dataSource, "order-intake"))
                    .skipDuplicate(false)
                    .filter(exchangeProperty(Exchange.DUPLICATE_MESSAGE).isEqualTo(true))
                        .log(LoggingLevel.WARN, "Duplicate order ${body.orderId} ignored")
                        .stop()
                    .end()
                    // Content enricher: customer master data and FX conversion from Postgres.
                    .bean(processor, "enrich")
                    // Wire tap: fire-and-forget audit copy that never slows the main flow.
                    .wireTap("direct:audit")
                    // Content-based router.
                    .choice()
                        .when(e -> processor.exceedsCreditLimit(e.getIn().getBody(Order.class)))
                            .bean(processor, "rejectForCredit")
                        .when(e -> processor.isExpress(e.getIn().getBody(Order.class)))
                            .process(e -> e.getIn().getBody(Order.class).approve("EXPRESS"))
                        .otherwise()
                            .process(e -> e.getIn().getBody(Order.class).approve("STANDARD"))
                    .end()
                    .bean(orders, "save")
                    .log("Order ${body.orderId} ${body.status} lane=${body.lane} amountUsd=${body.amountUsd}")
                    .setHeader(KafkaConstants.KEY, simple("${body.customerId}"))
                    .marshal().json(JsonLibrary.Jackson)
                    .convertBodyTo(String.class)
                    .to("kafka:{{orders.topic.processed}}")
                .end();

        from("direct:audit")
                .routeId("order-audit")
                .log(LoggingLevel.INFO, "audit", "AUDIT order=${body.orderId} customer=${body.customerId} "
                        + "amount=${body.amount} ${body.currency} source=${body.source} by=${body.submittedBy}");

        from("direct:dead-letter")
                .routeId("dead-letter")
                .process(e -> {
                    Exception cause = e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    e.getIn().setHeader("x-error-type", cause == null ? "Unknown" : cause.getClass().getSimpleName());
                    e.getIn().setHeader("x-error-message", cause == null ? "" : String.valueOf(cause.getMessage()));
                    e.getIn().setHeader("x-source-topic", e.getIn().getHeader(KafkaConstants.TOPIC));
                })
                .log(LoggingLevel.WARN, "Parking message in DLT: ${header.x-error-type}: ${header.x-error-message}")
                .convertBodyTo(String.class)
                .to("kafka:{{orders.topic.dlt}}");
    }
}
