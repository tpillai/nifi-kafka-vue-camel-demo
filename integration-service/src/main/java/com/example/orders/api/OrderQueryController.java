package com.example.orders.api;

import com.example.orders.model.Order;
import com.example.orders.persistence.OrderRepository;
import com.example.orders.persistence.ReferenceDataRepository;
import com.example.orders.persistence.ReferenceDataRepository.Customer;
import com.example.orders.persistence.ReferenceDataRepository.FxRate;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
@Validated
@Tag(name = "Orders", description = "Query processed orders and reference data")
public class OrderQueryController {

    private final OrderRepository orders;
    private final ReferenceDataRepository referenceData;

    public OrderQueryController(OrderRepository orders, ReferenceDataRepository referenceData) {
        this.orders = orders;
        this.referenceData = referenceData;
    }

    @GetMapping("/orders")
    @Operation(summary = "List processed orders, newest first")
    @ApiResponse(responseCode = "200", description = "Orders")
    @ApiResponse(responseCode = "401", description = "Missing or invalid token")
    @ApiResponse(responseCode = "403", description = "Token lacks role orders-read")
    public List<Order> list(
            @Parameter(description = "APPROVED or REJECTED") @RequestParam(required = false) String status,
            @Parameter(description = "Filter by customer, for example C-100") @RequestParam(required = false) String customerId,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return orders.find(status, customerId, limit);
    }

    @GetMapping("/orders/{orderId}")
    @Operation(summary = "Get one order by id",
            description = "Returns 404 until the Camel route has processed the order. Processing is asynchronous, "
                    + "so poll this after submitting, or listen on the gateway's SSE stream.")
    @ApiResponse(responseCode = "200", description = "The processed order")
    @ApiResponse(responseCode = "404", description = "Not processed yet, rejected to the DLT, or unknown id")
    public ResponseEntity<?> get(@PathVariable UUID orderId) {
        return orders.findById(orderId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,
                                "Order " + orderId + " has not been processed")));
    }

    @GetMapping("/customers")
    @Operation(summary = "List customers with tier and credit limit (USD)")
    public List<Customer> customers() {
        return referenceData.findAllCustomers();
    }

    @GetMapping("/fx-rates")
    @Operation(summary = "Current FX rates (units per 1 USD) and whether they came from the live feed or the seed")
    public List<FxRate> fxRates() {
        return referenceData.findAllRates();
    }
}
