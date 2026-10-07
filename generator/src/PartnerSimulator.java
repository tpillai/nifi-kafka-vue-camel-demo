import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simulates two external partners feeding the pipeline:
 *
 *  1. Partner API: an OAuth2 client-credentials client that POSTs orders to the Vert.x gateway.
 *  2. Partner file drop: writes CSV batches into a folder that NiFi polls (stand-in for SFTP).
 *
 * A configurable share of records is deliberately bad (unknown customer, over credit limit,
 * missing customerId, duplicated rows) so every error path in the pipeline gets exercised.
 * No dependencies beyond the JDK.
 */
public class PartnerSimulator {

    private static final Map<String, String> ENV = System.getenv();
    private static final String GATEWAY = env("GATEWAY_URL", "http://localhost:8080");
    private static final String TOKEN_URL = env("TOKEN_URL",
            "http://localhost:8180/realms/orders-poc/protocol/openid-connect/token");
    private static final String CLIENT_ID = env("CLIENT_ID", "partner-client");
    private static final String CLIENT_SECRET = env("CLIENT_SECRET", "partner-secret");
    private static final Path INBOX = Path.of(env("INBOX_DIR", "./data/inbox"));
    private static final long API_INTERVAL_MS = Long.parseLong(env("API_INTERVAL_MS", "5000"));
    private static final long FILE_INTERVAL_MS = Long.parseLong(env("FILE_INTERVAL_MS", "45000"));
    private static final int FILE_BATCH_SIZE = Integer.parseInt(env("FILE_BATCH_SIZE", "5"));
    private static final double BAD_RATIO = Double.parseDouble(env("BAD_RATIO", "0.1"));

    private static final String[] CUSTOMERS = {"C-100", "C-100", "C-200", "C-200", "C-300"};
    private static final String[] PRODUCTS = {"Widget", "Gadget", "Sprocket", "Flux capacitor", "Gizmo", "Bracket"};
    private static final String[] CURRENCIES = {"USD", "USD", "EUR", "GBP", "INR"};
    private static final Pattern ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXPIRES_IN = Pattern.compile("\"expires_in\"\\s*:\\s*(\\d+)");

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private String token;
    private Instant tokenExpiry = Instant.EPOCH;

    public static void main(String[] args) {
        PartnerSimulator sim = new PartnerSimulator();
        var scheduler = Executors.newScheduledThreadPool(2);
        if (API_INTERVAL_MS > 0) {
            scheduler.scheduleWithFixedDelay(sim::safeSendApiOrder, 10_000, API_INTERVAL_MS, TimeUnit.MILLISECONDS);
            log("API partner: one order every %d ms via %s", API_INTERVAL_MS, GATEWAY);
        }
        if (FILE_INTERVAL_MS > 0) {
            scheduler.scheduleWithFixedDelay(sim::safeDropFile, 15_000, FILE_INTERVAL_MS, TimeUnit.MILLISECONDS);
            log("File partner: %d orders every %d ms into %s", FILE_BATCH_SIZE, FILE_INTERVAL_MS, INBOX.toAbsolutePath());
        }
    }

    // ------------------------------------------------------------------ channel 1: secured REST API

    private void safeSendApiOrder() {
        try {
            sendApiOrder();
        } catch (Exception e) {
            log("API order failed: %s", e.getMessage());
        }
    }

    private void sendApiOrder() throws IOException, InterruptedException {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        String customer = r.nextDouble() < BAD_RATIO ? "C-999" : pick(CUSTOMERS);
        String body = """
                {"customerId":"%s","product":"%s","quantity":%d,"amount":%s,"currency":"%s","priority":"%s"}"""
                .formatted(customer, pick(PRODUCTS), r.nextInt(1, 10), amount(), pick(CURRENCIES),
                        r.nextDouble() < 0.2 ? "HIGH" : "NORMAL");

        HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create(GATEWAY + "/api/orders"))
                .timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + token())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
        log("API  -> %d %s", resp.statusCode(), resp.body());
        if (resp.statusCode() == 401) {
            tokenExpiry = Instant.EPOCH;
        }
    }

    /** Client-credentials grant, cached until shortly before expiry. */
    private String token() throws IOException, InterruptedException {
        if (token != null && Instant.now().isBefore(tokenExpiry)) {
            return token;
        }
        String form = "grant_type=client_credentials&client_id=" + enc(CLIENT_ID) + "&client_secret=" + enc(CLIENT_SECRET);
        HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create(TOKEN_URL))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build(), HttpResponse.BodyHandlers.ofString());
        Matcher m = ACCESS_TOKEN.matcher(resp.body());
        if (resp.statusCode() != 200 || !m.find()) {
            throw new IOException("token request failed: " + resp.statusCode() + " " + resp.body());
        }
        token = m.group(1);
        Matcher exp = EXPIRES_IN.matcher(resp.body());
        long seconds = exp.find() ? Long.parseLong(exp.group(1)) : 60;
        tokenExpiry = Instant.now().plusSeconds(Math.max(10, seconds - 30));
        log("Obtained client-credentials token for %s (expires in %ds)", CLIENT_ID, seconds);
        return token;
    }

    // ------------------------------------------------------------------ channel 2: CSV file drop

    private void safeDropFile() {
        try {
            dropFile();
        } catch (Exception e) {
            log("File drop failed: %s", e.getMessage());
        }
    }

    private void dropFile() throws IOException {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        List<String> rows = new ArrayList<>();
        rows.add("orderId,customerId,product,quantity,amount,currency,priority,source,submittedBy,receivedAt");
        for (int i = 0; i < FILE_BATCH_SIZE; i++) {
            boolean bad = r.nextDouble() < BAD_RATIO;
            String customer = bad && r.nextBoolean() ? "" : pick(CUSTOMERS);   // empty -> NiFi rejects it
            rows.add(String.join(",", UUID.randomUUID().toString(), customer, pick(PRODUCTS),
                    String.valueOf(r.nextInt(1, 20)), amount(), pick(CURRENCIES),
                    r.nextDouble() < 0.2 ? "HIGH" : "NORMAL", "file-drop", "partner-sftp", Instant.now().toString()));
        }
        if (r.nextDouble() < BAD_RATIO * 3) {
            rows.add(rows.get(1));  // partner resends a row: the idempotent consumer must drop it
        }

        Files.createDirectories(INBOX);
        String name = "partner-" + System.currentTimeMillis() + ".csv";
        // Write to a hidden temp file first; NiFi ignores dot-files, so it never reads a half-written batch.
        Path tmp = INBOX.resolve("." + name + ".tmp");
        Files.write(tmp, rows, StandardCharsets.UTF_8);
        Files.move(tmp, INBOX.resolve(name), StandardCopyOption.ATOMIC_MOVE);
        log("FILE -> %s (%d rows)", name, rows.size() - 1);
    }

    // ------------------------------------------------------------------ helpers

    private static String amount() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        double value = r.nextDouble() < 0.1 ? r.nextDouble(2_000, 20_000) : r.nextDouble(5, 900);
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    private static String pick(String[] values) {
        return values[ThreadLocalRandom.current().nextInt(values.length)];
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String env(String key, String fallback) {
        return ENV.getOrDefault(key, fallback);
    }

    private static void log(String fmt, Object... args) {
        System.out.println(Instant.now() + " " + String.format(fmt, args));
    }
}
