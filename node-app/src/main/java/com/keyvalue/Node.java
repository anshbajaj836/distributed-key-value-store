package com.keyvalue;

import io.javalin.Javalin;
import io.javalin.http.Context;
import okhttp3.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Node {
    // 1. THE DATABASE (In-Memory Key-Value Store)
    private static final Map<String, String> kvStore = new ConcurrentHashMap<>();

    // 2. STATE
    private static final Map<Integer, Long> peerLastSeen = new ConcurrentHashMap<>();
    private static volatile int currentLeaderId = -1; // Who is the boss?

    // Configuration
    private static final OkHttpClient client = new OkHttpClient.Builder().readTimeout(1, TimeUnit.SECONDS).build();
    private static int MY_ID;
    private static List<String> PEER_HOSTNAMES;

    public static void main(String[] args) {
        MY_ID = Integer.parseInt(System.getenv().getOrDefault("NODE_ID", "1"));
        String peersEnv = System.getenv().getOrDefault("PEERS", "");
        PEER_HOSTNAMES = peersEnv.isEmpty() ? List.of() : Arrays.asList(peersEnv.split(","));

        System.out.println("--- NODE " + MY_ID + " STARTED ---");

        Javalin app = Javalin.create().start(7000);

        // --- ENDPOINTS ---

        // 1. Health Check
        app.get("/ping", ctx -> ctx.result("Pong from " + MY_ID));

        // 2. Public Write Endpoint (The Entry Point)
        app.post("/data/{key}/{value}", ctx -> handleWriteRequest(ctx));

        // 3. Public Read Endpoint
        app.get("/data/{key}", ctx -> {
            String value = kvStore.get(ctx.pathParam("key"));
            if (value == null) ctx.status(404).result("Key not found on Node " + MY_ID);
            else ctx.result(value);
        });

        // 4. Internal Replication Endpoint (Only for Leader -> Follower)
        app.post("/internal/replicate/{key}/{value}", ctx -> {
            String key = ctx.pathParam("key");
            String value = ctx.pathParam("value");
            kvStore.put(key, value);
            System.out.println("--- [Replication] Leader told me to save " + key + "=" + value + " ---");
            ctx.status(200);
        });

        // --- BACKGROUND TASKS ---
        new Thread(Node::runFailureDetector).start();
        new Thread(Node::runLeaderElection).start();
    }

    // --- LOGIC HANDLERS ---

    private static void handleWriteRequest(Context ctx) {
        String key = ctx.pathParam("key");
        String value = ctx.pathParam("value");

        // CASE A: I am not the leader -> Redirect
        if (currentLeaderId != MY_ID) {
            if (currentLeaderId == -1) {
                ctx.status(503).result("No leader available");
                return;
            }
            // Redirect client to the leader using host-mapped port (host: 7000 + leaderId)
            int leaderHostPort = 7000 + currentLeaderId; // host ports: 7001,7002,7003
            String leaderUrl = "http://localhost:" + leaderHostPort + "/data/" + key + "/" + value;
            ctx.status(307).redirect(leaderUrl);
            System.out.println("--- [Redirect] Redirecting client to Leader (Node " + currentLeaderId + ") at " + leaderUrl + " ---");
            return;
        }

        // CASE B: I am the leader -> Save and Replicate
        System.out.println("--- [Write] I am Leader. Saving " + key + "=" + value + " ---");

        // 1. Save Locally
        kvStore.put(key, value);

        // 2. Replicate to others (Fire and Forget for now)
        for (String peer : PEER_HOSTNAMES) {
            replicateToPeer(peer, key, value);
        }

        ctx.result("Write Successful on Leader " + MY_ID);
    }

    private static void replicateToPeer(String peerHost, String key, String value) {
        // Send to /internal/replicate endpoint
        String url = "http://" + peerHost + ":7000/internal/replicate/" + key + "/" + value;
        Request request = new Request.Builder().url(url).post(RequestBody.create(new byte[0])).build();

        // Asynchronous call (don't wait for response to keep it simple)
        client.newCall(request).enqueue(new Callback() {
            @Override public void onFailure(Call call, IOException e) {
                System.out.println("Failed to replicate to " + peerHost);
            }
            @Override public void onResponse(Call call, Response response) {
                response.close();
            }
        });
    }

    // --- BACKGROUND THREADS (Same as before) ---

    private static void runFailureDetector() {
        while (true) {
            for (String peerHost : PEER_HOSTNAMES) {
                int peerId = Integer.parseInt(peerHost.split("-")[1]);
                if (pingPeer(peerHost)) peerLastSeen.put(peerId, System.currentTimeMillis());
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
        }
    }

    private static void runLeaderElection() {
        while (true) {
            long now = System.currentTimeMillis();
            List<Integer> aliveNodes = new ArrayList<>();
            aliveNodes.add(MY_ID);
            peerLastSeen.forEach((id, lastSeen) -> {
                if (now - lastSeen < 5000) aliveNodes.add(id);
            });

            int newLeader = Collections.max(aliveNodes);
            if (newLeader != currentLeaderId) {
                System.out.println("--- [Election] New Leader is Node " + newLeader + " ---");
                currentLeaderId = newLeader;
            }
            try { Thread.sleep(2000); } catch (InterruptedException e) {}
        }
    }

    private static boolean pingPeer(String host) {
        Request request = new Request.Builder().url("http://" + host + ":7000/ping").build();
        try (Response response = client.newCall(request).execute()) { return response.isSuccessful(); }
        catch (Exception e) { return false; }
    }
}