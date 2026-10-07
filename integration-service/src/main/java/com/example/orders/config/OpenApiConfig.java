package com.example.orders.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
        info = @Info(title = "Order Query API", version = "0.1.0",
                description = "Read side of the order POC. Orders are written asynchronously by the Camel routes "
                        + "(Kafka orders.raw to Postgres); this API only reads. Requires a Keycloak access token "
                        + "with realm role orders-read and audience orders-api."),
        servers = @Server(url = "http://localhost:8081", description = "Direct (bypassing the gateway)"),
        security = @SecurityRequirement(name = "keycloak"))
@SecurityScheme(name = "keycloak", type = SecuritySchemeType.HTTP, scheme = "bearer", bearerFormat = "JWT",
        description = "Paste an access token from Keycloak, for example from scripts/get-token.sh")
public class OpenApiConfig {
}
