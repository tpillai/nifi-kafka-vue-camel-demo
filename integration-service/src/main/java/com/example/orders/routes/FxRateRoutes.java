package com.example.orders.routes;

import com.example.orders.model.FxRateMessage;
import com.example.orders.persistence.ReferenceDataRepository;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Reference data feed: NiFi polls the public Frankfurter (ECB) API and publishes the payload to fx.rates.
 * Kept in its own RouteBuilder so its error handling stays separate from the order DLT.
 */
@Component
public class FxRateRoutes extends RouteBuilder {

    private final ReferenceDataRepository referenceData;

    public FxRateRoutes(ReferenceDataRepository referenceData) {
        this.referenceData = referenceData;
    }

    @Override
    public void configure() {
        // A bad rate snapshot is skipped: the next poll a minute later supersedes it.
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(2)
                .redeliveryDelay(2000)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        from("kafka:{{fx.topic.rates}}?groupId=integration-service-fx&autoOffsetReset=earliest")
                .routeId("fx-rates")
                .unmarshal().json(JsonLibrary.Jackson, FxRateMessage.class)
                .process(e -> {
                    FxRateMessage msg = e.getIn().getBody(FxRateMessage.class);
                    LocalDate asOf = LocalDate.parse(msg.date());
                    referenceData.upsertRate(msg.base(), BigDecimal.ONE, asOf, "frankfurter");
                    msg.rates().forEach((currency, rate) ->
                            referenceData.upsertRate(currency, rate, asOf, "frankfurter"));
                })
                .log("FX rates updated from ${body.base} feed dated ${body.date}: ${body.rates}");
    }
}
