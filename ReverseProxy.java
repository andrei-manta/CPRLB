import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ReverseProxy {

    static final int    PORT         = 8080;
    static final int    THREADS      = 16;
    static final int    CONNECT_TO   = 5_000;
    static final int    READ_TO      = 10_000;
    static final long   CACHE_TTL_MS = 30_000;
    static final int    CACHE_MAX    = 512;

    static final String[] BACKENDS = {
        "http://127.0.0.1:8001",
        "http://127.0.0.1:8002",
        "http://127.0.0.1:8003",
    };

    record CacheEntry(byte[] body, String contentType, int status, Instant expiresAt) {
        boolean expired() { return Instant.now().isAfter(expiresAt); }
    }

    record ParsedRequest(String method, String path, String version,
                         Map<String, String> headers, byte[] body) {}

    static final AtomicInteger                   rr    = new AtomicInteger(0);
    static final LinkedHashMap<String, CacheEntry> cache = new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> e) {
            return size() > CACHE_MAX;
        }
    };
    static final Object cacheLock = new Object();

    public static void main(String[] args) throws IOException {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        ServerSocket server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(PORT));

        System.out.printf("Proxy listening on :%d → %s%n", PORT, Arrays.toString(BACKENDS));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.close(); } catch (IOException ignored) {}
            pool.shutdown();
            System.out.println("\nShutdown complete.");
        }));

        while (!server.isClosed()) {
            try {
                Socket client = server.accept();
                pool.submit(() -> handle(client));
            } catch (IOException e) {
                if (!server.isClosed()) e.printStackTrace();
            }
        }
    }

    static void handle(Socket client) {
        try (client) {
            client.setSoTimeout(READ_TO);
            InputStream  in  = client.getInputStream();
            OutputStream out = client.getOutputStream();

            ParsedRequest req = parseRequest(in);
            if (req == null) { sendError(out, 400, "Bad Request"); return; }

            boolean cacheable = req.method().equals("GET") || req.method().equals("HEAD");
            String  cacheKey  = req.method() + " " + req.path();

            if (cacheable) {
                synchronized (cacheLock) {
                    CacheEntry entry = cache.get(cacheKey);
                    if (entry != null && !entry.expired()) {
                        sendCached(out, entry, req.method().equals("HEAD"));
                        return;
                    }
                    cache.remove(cacheKey);
                }
            }

            String backend = pickBackend();
            URL    target  = new URL(backend + req.path());

            HttpURLConnection conn = (HttpURLConnection) target.openConnection();
            conn.setConnectTimeout(CONNECT_TO);
            conn.setReadTimeout(READ_TO);
            conn.setRequestMethod(req.method());
            conn.setDoOutput(req.body() != null && req.body().length > 0);
            conn.setInstanceFollowRedirects(false);

            for (Map.Entry<String, String> h : req.headers().entrySet()) {
                String k = h.getKey();
                if (k.equalsIgnoreCase("host") || k.equalsIgnoreCase("connection")) continue;
                conn.setRequestProperty(k, h.getValue());
            }
            conn.setRequestProperty("X-Forwarded-For",
                client.getInetAddress().getHostAddress());

            if (conn.getDoOutput()) {
                try (OutputStream bo = conn.getOutputStream()) {
                    bo.write(req.body());
                }
            }

            int status;
            try {
                status = conn.getResponseCode();
            } catch (IOException e) {
                sendError(out, 502, "Bad Gateway");
                return;
            }

            Map<String, String> respHeaders = new LinkedHashMap<>();
            for (int i = 1; ; i++) {
                String key = conn.getHeaderFieldKey(i);
                if (key == null) break;
                respHeaders.put(key, conn.getHeaderField(i));
            }

            InputStream bodyStream = status >= 400
                ? conn.getErrorStream()
                : conn.getInputStream();

            byte[] body = bodyStream == null ? new byte[0] : bodyStream.readAllBytes();
            String contentType = respHeaders.getOrDefault("Content-Type", "application/octet-stream");

            if (cacheable && status == 200) {
                CacheEntry entry = new CacheEntry(body, contentType, status,
                    Instant.now().plusMillis(CACHE_TTL_MS));
                synchronized (cacheLock) { cache.put(cacheKey, entry); }
            }

            StringBuilder sb = new StringBuilder();
            sb.append("HTTP/1.1 ").append(status).append(" ")
              .append(statusText(status)).append("\r\n");
            for (Map.Entry<String, String> h : respHeaders.entrySet()) {
                String k = h.getKey();
                if (k.equalsIgnoreCase("transfer-encoding")) continue;
                sb.append(k).append(": ").append(h.getValue()).append("\r\n");
            }
            sb.append("Content-Length: ").append(body.length).append("\r\n");
            sb.append("X-Proxy-Backend: ").append(backend).append("\r\n");
            sb.append("Connection: close\r\n\r\n");

            out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
            if (!req.method().equals("HEAD")) out.write(body);
            out.flush();

        } catch (IOException e) {
            // client disconnected mid-transfer — nothing to do
        }
    }

    static ParsedRequest parseRequest(InputStream in) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII));

        String requestLine = reader.readLine();
        if (requestLine == null || requestLine.isBlank()) return null;

        String[] parts = requestLine.split(" ", 3);
        if (parts.length != 3) return null;

        String method  = parts[0];
        String path    = parts[1];
        String version = parts[2];

        Map<String, String> headers = new LinkedHashMap<>();
        String line;
        while ((line = reader.readLine()) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon < 0) continue;
            headers.put(line.substring(0, colon).trim(), line.substring(colon + 1).trim());
        }

        byte[] body = new byte[0];
        String lenStr = headers.get("Content-Length");
        if (lenStr != null) {
            int len = Integer.parseInt(lenStr.trim());
            body = new byte[len];
            int read = 0;
            while (read < len) {
                int r = in.read(body, read, len - read);
                if (r < 0) break;
                read += r;
            }
        }

        return new ParsedRequest(method, path, version, headers, body);
    }

    static String pickBackend() {
        int idx = Math.abs(rr.getAndIncrement() % BACKENDS.length);
        return BACKENDS[idx];
    }

    static void sendError(OutputStream out, int status, String msg) {
        try {
            byte[] body = msg.getBytes(StandardCharsets.UTF_8);
            String resp = "HTTP/1.1 " + status + " " + msg + "\r\n"
                + "Content-Type: text/plain\r\n"
                + "Content-Length: " + body.length + "\r\n"
                + "Connection: close\r\n\r\n";
            out.write(resp.getBytes(StandardCharsets.US_ASCII));
            out.write(body);
            out.flush();
        } catch (IOException ignored) {}
    }

    static void sendCached(OutputStream out, CacheEntry entry, boolean headOnly) {
        try {
            String resp = "HTTP/1.1 " + entry.status() + " " + statusText(entry.status()) + "\r\n"
                + "Content-Type: " + entry.contentType() + "\r\n"
                + "Content-Length: " + entry.body().length + "\r\n"
                + "X-Cache: HIT\r\n"
                + "Connection: close\r\n\r\n";
            out.write(resp.getBytes(StandardCharsets.US_ASCII));
            if (!headOnly) out.write(entry.body());
            out.flush();
        } catch (IOException ignored) {}
    }

    static String statusText(int code) {
        return switch (code) {
            case 200 -> "OK";
            case 201 -> "Created";
            case 204 -> "No Content";
            case 301 -> "Moved Permanently";
            case 302 -> "Found";
            case 304 -> "Not Modified";
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized";
            case 403 -> "Forbidden";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 500 -> "Internal Server Error";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            default  -> "Unknown";
        };
    }
}
