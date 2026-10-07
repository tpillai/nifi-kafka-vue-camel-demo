package com.example.gateway;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Map;

public final class GatewayApp {

    private GatewayApp() {
    }

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        JsonObject config = new JsonObject()
                .put("port", Integer.parseInt(env.getOrDefault("PORT", "8080")))
                .put("publicUrl", env.getOrDefault("PUBLIC_URL", "http://localhost:8080"))
                .put("issuer", env.getOrDefault("OIDC_ISSUER", "http://localhost:8180/realms/orders-poc"))
                .put("internalIssuer", env.getOrDefault("OIDC_INTERNAL_ISSUER", "http://localhost:8180/realms/orders-poc"))
                .put("clientId", env.getOrDefault("OIDC_CLIENT_ID", "gateway-ui"))
                .put("clientSecret", env.getOrDefault("OIDC_CLIENT_SECRET", "gateway-ui-secret"))
                .put("audience", env.getOrDefault("OIDC_AUDIENCE", "orders-api"))
                .put("nifiUrl", env.getOrDefault("NIFI_INGEST_URL", "http://localhost:9090/orders"))
                .put("ordersApiUrl", env.getOrDefault("ORDERS_API_URL", "http://localhost:8081"))
                .put("kafkaBootstrap", env.getOrDefault("KAFKA_BOOTSTRAP", "localhost:9094"))
                .put("rateLimitPerMinute", Integer.parseInt(env.getOrDefault("RATE_LIMIT_PER_MINUTE", "30")));

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new GatewayVerticle(), new DeploymentOptions().setConfig(config))
                .onSuccess(id -> System.out.println("Gateway listening on port " + config.getInteger("port")))
                .onFailure(err -> {
                    err.printStackTrace();
                    System.exit(1);
                });
    }
}
