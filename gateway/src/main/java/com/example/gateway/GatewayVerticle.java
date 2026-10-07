package com.example.gateway;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VerticleBase;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Single verticle that plays three roles:
 * BFF for the browser (OIDC code flow + PKCE, tokens kept server-side in the session),
 * API gateway for machine clients (Bearer JWT), and push channel (Kafka events to SSE over the event bus).
 * Everything is non-blocking; nothing here may block the event loop.
 */
public class GatewayVerticle extends VerticleBase {

    private static final String EVENTS_ADDRESS = "order.events";
    private static final String CTX_CLAIMS = "claims";
    private static final String CTX_TOKEN = "token";
    private static final Pattern CUSTOMER_ID = Pattern.compile("[A-Z]-\\d{3}");
    private static final Set<String> CURRENCIES = Set.of("USD", "EUR", "GBP", "INR");

    private OAuth2Auth oauth2;
    private WebClient http;
    private KafkaConsumer<String, String> events;
    private final Map<String, long[]> rateWindows = new ConcurrentHashMap<>();

    @Override
    public Future<?> start() {
        JsonObject cfg = config();
        String issuer = cfg.getString("issuer");
        String internal = cfg.getString("internalIssuer");
        http = WebClient.create(vertx);

        // Browser-facing endpoints use the public issuer URL; server-to-Keycloak calls use the in-network one.
        // The JWT "iss" claim is always the public URL (Keycloak runs with KC_HOSTNAME set).
        oauth2 = OAuth2Auth.create(vertx, new OAuth2Options()
                .setClientId(cfg.getString("clientId"))
                .setClientSecret(cfg.getString("clientSecret"))
                .setSite(issuer)
                .setAuthorizationPath(issuer + "/protocol/openid-connect/auth")
                .setLogoutPath(issuer + "/protocol/openid-connect/logout")
                .setTokenPath(internal + "/protocol/openid-connect/token")
                .setJwkPath(internal + "/protocol/openid-connect/certs")
                .setUserInfoPath(internal + "/protocol/openid-connect/userinfo")
                .setJWTOptions(new JWTOptions()
                        .setIssuer(issuer)
                        .setAudience(List.of(cfg.getString("audience")))
                        .setLeeway(5)));

        return loadKeys(30)
                .compose(v -> startKafkaBridge(cfg))
                .compose(v -> vertx.createHttpServer()
                        .requestHandler(router(cfg))
                        .listen(cfg.getInteger("port")));
    }

    /** Keycloak may still be importing the realm when we start, so retry fetching the JWKS. */
    private Future<Void> loadKeys(int attemptsLeft) {
        return oauth2.jWKSet().recover(err -> {
            if (attemptsLeft <= 0) {
                return Future.failedFuture(err);
            }
            System.out.println("JWKS not available yet (" + err.getMessage() + "), retrying");
            Promise<Void> retry = Promise.promise();
            vertx.setTimer(2000, t -> loadKeys(attemptsLeft - 1).onComplete(retry));
            return retry.future();
        });
    }

    private Router router(JsonObject cfg) {
        Router router = Router.router(vertx);
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

        // Browser login: authorization code + PKCE. Tokens never reach the browser.
        OAuth2AuthHandler login = OAuth2AuthHandler.create(vertx, oauth2, cfg.getString("publicUrl") + "/callback")
                .setupCallback(router.get("/callback"))
                .withScope("openid")
                .withScope("profile")
                .pkceVerifierLength(64);
        router.get("/login").handler(login).handler(ctx -> ctx.redirect("/"));
        router.get("/logout").handler(this::logout);

        router.get("/health").handler(ctx -> json(ctx.response(), 200, new JsonObject().put("status", "UP")));

        // Body handler first: it must run before any async handler (authentication) touches the request.
        router.route("/api/*").handler(BodyHandler.create().setBodyLimit(16 * 1024));
        router.route("/api/*").handler(this::authenticate);
        router.get("/api/me").handler(this::me);
        router.get("/api/events").handler(this::requireRead).handler(this::sse);
        router.post("/api/orders")
                .handler(this::rateLimit)
                .handler(ctx -> requireRole(ctx, "orders-write"))
                .handler(this::submitOrder);
        for (String path : List.of("/api/orders", "/api/orders/:orderId", "/api/customers", "/api/fx-rates")) {
            router.get(path).handler(this::requireRead).handler(this::proxyToOrdersApi);
        }

        router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));
        return router;
    }

    // ---------------------------------------------------------------- authentication and authorisation

    /**
     * Accepts either a Bearer token (machine clients) or the browser session (BFF).
     * Bearer tokens are verified locally against cached JWKS: signature, exp, iss and aud.
     */
    private void authenticate(RoutingContext ctx) {
        String header = ctx.request().getHeader("Authorization");
        if (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = header.substring(7).trim();
            oauth2.authenticate(new TokenCredentials(token))
                    .onSuccess(user -> accept(ctx, user, token))
                    .onFailure(err -> problem(ctx, 401, "Invalid token: " + err.getMessage()));
            return;
        }
        User user = ctx.user();
        if (user == null) {
            problem(ctx, 401, "Log in at /login or send a Bearer token");
            return;
        }
        if (user.expired(5)) {
            problem(ctx, 401, "Session token expired; log in again at /login");
            return;
        }
        accept(ctx, user, user.principal().getString("access_token"));
    }

    private void accept(RoutingContext ctx, User user, String token) {
        ctx.put(CTX_TOKEN, token);
        ctx.put(CTX_CLAIMS, claims(user, token));
        ctx.next();
    }

    private static JsonObject claims(User user, String token) {
        JsonObject accessToken = user.attributes().getJsonObject("accessToken");
        if (accessToken != null) {
            return accessToken;
        }
        // Already verified by the provider; just read the payload.
        String payload = token.split("\\.")[1];
        return new JsonObject(new String(Base64.getUrlDecoder().decode(payload)));
    }

    private static List<String> roles(RoutingContext ctx) {
        JsonObject realmAccess = ctx.<JsonObject>get(CTX_CLAIMS).getJsonObject("realm_access", new JsonObject());
        return realmAccess.getJsonArray("roles", new JsonArray()).stream().map(String::valueOf).toList();
    }

    private static String subject(RoutingContext ctx) {
        JsonObject claims = ctx.get(CTX_CLAIMS);
        return claims.getString("preferred_username", claims.getString("sub"));
    }

    private void requireRead(RoutingContext ctx) {
        requireRole(ctx, "orders-read");
    }

    private void requireRole(RoutingContext ctx, String role) {
        if (roles(ctx).contains(role)) {
            ctx.next();
        } else {
            problem(ctx, 403, subject(ctx) + " lacks role " + role);
        }
    }

    /** Fixed one-minute window per caller. In-memory, so per gateway instance; fine for a POC. */
    private void rateLimit(RoutingContext ctx) {
        int limit = config().getInteger("rateLimitPerMinute");
        long window = System.currentTimeMillis() / 60_000;
        long[] state = rateWindows.compute(subject(ctx), (k, v) ->
                v == null || v[0] != window ? new long[]{window, 1} : new long[]{window, v[1] + 1});
        long remaining = Math.max(0, limit - state[1]);
        ctx.response().putHeader("X-RateLimit-Limit", String.valueOf(limit))
                .putHeader("X-RateLimit-Remaining", String.valueOf(remaining));
        if (state[1] > limit) {
            ctx.response().putHeader("Retry-After", String.valueOf(60 - (System.currentTimeMillis() / 1000) % 60));
            problem(ctx, 429, "Rate limit of " + limit + " orders per minute exceeded");
        } else {
            ctx.next();
        }
    }

    private void me(RoutingContext ctx) {
        JsonObject claims = ctx.get(CTX_CLAIMS);
        json(ctx.response(), 200, new JsonObject()
                .put("username", subject(ctx))
                .put("name", claims.getString("name"))
                .put("clientId", claims.getString("azp"))
                .put("roles", new JsonArray(roles(ctx)))
                .put("tokenExpiresAt", Instant.ofEpochSecond(claims.getLong("exp")).toString()));
    }

    private void logout(RoutingContext ctx) {
        User user = ctx.user();
        String url = "/";
        if (user != null) {
            url = oauth2.endSessionURL(user, new JsonObject()
                    .put("post_logout_redirect_uri", config().getString("publicUrl") + "/"));
        }
        if (ctx.session() != null) {
            ctx.session().destroy();
        }
        ctx.redirect(url);
    }

    // ---------------------------------------------------------------- write path: gateway -> NiFi

    private void submitOrder(RoutingContext ctx) {
        JsonObject in;
        try {
            in = ctx.body().asJsonObject();
        } catch (DecodeException e) {
            problem(ctx, 400, "Body must be a JSON object");
            return;
        }
        if (in == null) {
            problem(ctx, 400, "Body must be a JSON object");
            return;
        }
        String error = validate(in);
        if (error != null) {
            problem(ctx, 400, error);
            return;
        }

        JsonObject claims = ctx.get(CTX_CLAIMS);
        String orderId = UUID.randomUUID().toString();
        JsonObject order = new JsonObject()
                .put("orderId", orderId)
                .put("customerId", in.getString("customerId"))
                .put("product", in.getString("product"))
                .put("quantity", in.getInteger("quantity"))
                .put("amount", new BigDecimal(String.valueOf(in.getValue("amount"))))
                .put("currency", in.getString("currency", "USD").toUpperCase())
                .put("priority", in.getString("priority", "NORMAL").toUpperCase())
                .put("source", "partner-client".equals(claims.getString("azp")) ? "partner-api" : "web-ui")
                .put("submittedBy", subject(ctx))
                .put("receivedAt", Instant.now().toString());

        http.postAbs(config().getString("nifiUrl"))
                .timeout(5000)
                .sendJsonObject(order)
                .onSuccess(resp -> {
                    if (resp.statusCode() == 200) {
                        json(ctx.response().putHeader("Location", "/api/orders/" + orderId), 202, new JsonObject()
                                .put("orderId", orderId)
                                .put("status", "ACCEPTED")
                                .put("statusUrl", "/api/orders/" + orderId));
                    } else {
                        // ListenHTTP answers 503 when its outbound queue hits the backpressure threshold.
                        problem(ctx, 503, "Ingestion is busy (NiFi returned " + resp.statusCode() + "); retry later");
                    }
                })
                .onFailure(err -> problem(ctx, 503, "Ingestion unavailable: " + err.getMessage()));
    }

    /** Cheap shape checks at the edge; business rules (customer exists, credit limit) live in Camel. */
    static String validate(JsonObject in) {
        String customerId = in.getValue("customerId") instanceof String s ? s : null;
        if (customerId == null || !CUSTOMER_ID.matcher(customerId).matches()) {
            return "customerId is required and must look like C-100";
        }
        if (!(in.getValue("product") instanceof String p) || p.isBlank() || p.length() > 128) {
            return "product is required (max 128 characters)";
        }
        if (!(in.getValue("quantity") instanceof Number q) || q.intValue() < 1 || q.intValue() > 10_000) {
            return "quantity must be an integer between 1 and 10000";
        }
        if (!(in.getValue("amount") instanceof Number a) || a.doubleValue() <= 0) {
            return "amount must be a positive number";
        }
        Object currency = in.getValue("currency");
        if (currency != null && !(currency instanceof String c && CURRENCIES.contains(c.toUpperCase()))) {
            return "currency must be one of " + CURRENCIES;
        }
        Object priority = in.getValue("priority");
        if (priority != null && !(priority instanceof String pr && Set.of("HIGH", "NORMAL").contains(pr.toUpperCase()))) {
            return "priority must be HIGH or NORMAL";
        }
        return null;
    }

    // ---------------------------------------------------------------- read path: gateway -> Spring Boot

    /** Token passthrough: the user's own token goes to the downstream service, which validates it again. */
    private void proxyToOrdersApi(RoutingContext ctx) {
        String uri = ctx.request().uri();
        http.getAbs(config().getString("ordersApiUrl") + uri)
                .timeout(5000)
                .bearerTokenAuthentication(ctx.get(CTX_TOKEN))
                .send()
                .onSuccess(resp -> relay(ctx, resp))
                .onFailure(err -> problem(ctx, 502, "Orders API unavailable: " + err.getMessage()));
    }

    private static void relay(RoutingContext ctx, HttpResponse<Buffer> resp) {
        HttpServerResponse out = ctx.response().setStatusCode(resp.statusCode());
        String type = resp.getHeader("Content-Type");
        if (type != null) {
            out.putHeader("Content-Type", type);
        }
        out.end(resp.bodyAsBuffer() == null ? Buffer.buffer() : resp.bodyAsBuffer());
    }

    // ---------------------------------------------------------------- push path: Kafka -> event bus -> SSE

    /**
     * One Kafka consumer per gateway instance (unique group id, so every instance sees every event),
     * fanned out to browsers via the event bus.
     */
    private Future<Void> startKafkaBridge(JsonObject cfg) {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = UUID.randomUUID().toString();
        }
        events = KafkaConsumer.create(vertx, Map.of(
                "bootstrap.servers", cfg.getString("kafkaBootstrap"),
                "group.id", "gateway-" + host,
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "auto.offset.reset", "latest",
                "enable.auto.commit", "true"));
        events.handler(this::publishEvent);
        events.exceptionHandler(err -> System.err.println("Kafka consumer error: " + err.getMessage()));
        return events.subscribe(Set.of("orders.processed", "orders.raw.DLT", "orders.invalid"));
    }

    private void publishEvent(KafkaConsumerRecord<String, String> record) {
        String type = switch (record.topic()) {
            case "orders.processed" -> "processed";
            case "orders.raw.DLT" -> "dead-letter";
            default -> "invalid";
        };
        JsonObject event = new JsonObject().put("type", type).put("key", record.key()).put("at", Instant.now().toString());
        try {
            event.put("order", new JsonObject(record.value()));
        } catch (DecodeException e) {
            event.put("raw", record.value());
        }
        for (KafkaHeader h : record.headers()) {
            if (h.key().startsWith("x-error")) {
                event.put(h.key(), h.value().toString());
            }
        }
        vertx.eventBus().publish(EVENTS_ADDRESS, event);
    }

    private void sse(RoutingContext ctx) {
        HttpServerResponse resp = ctx.response()
                .setChunked(true)
                .putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .putHeader("X-Accel-Buffering", "no");
        resp.write(": connected\n\n");
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(EVENTS_ADDRESS, msg ->
                resp.write("event: " + msg.body().getString("type") + "\ndata: " + msg.body().encode() + "\n\n"));
        long keepAlive = vertx.setPeriodic(15_000, t -> resp.write(": ping\n\n"));
        resp.closeHandler(v -> {
            consumer.unregister();
            vertx.cancelTimer(keepAlive);
        });
    }

    // ---------------------------------------------------------------- helpers

    private static void json(HttpServerResponse resp, int status, JsonObject body) {
        resp.setStatusCode(status).putHeader("Content-Type", "application/json").end(body.encode());
    }

    /** RFC 9457 problem details, same shape the Spring service returns. */
    private static void problem(RoutingContext ctx, int status, String detail) {
        ctx.response().setStatusCode(status)
                .putHeader("Content-Type", "application/problem+json")
                .end(new JsonObject()
                        .put("type", "about:blank")
                        .put("title", titleFor(status))
                        .put("status", status)
                        .put("detail", detail)
                        .put("instance", ctx.request().path())
                        .encode());
    }

    private static String titleFor(int status) {
        return switch (status) {
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized";
            case 403 -> "Forbidden";
            case 429 -> "Too Many Requests";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            default -> "Error";
        };
    }

    @Override
    public Future<?> stop() {
        return events == null ? Future.succeededFuture() : events.close();
    }
}
