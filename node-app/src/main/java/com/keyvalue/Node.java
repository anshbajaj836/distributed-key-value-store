package com.keyvalue;

import io.javalin.Javalin;
import okhttp3.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Node {
    // 1. STATE: Keep track of when we last saw each peer
    private static final Map<Integer, Long> peerLastSeen = new ConcurrentHashMap<>();

    // Configuration
    private static final OkHttpClient client = new OkHttpClient.Builder().readTimeout(1, TimeUnit.SECONDS).build();

    private static int MY_ID;
    private static List<String> PEER_HOSTNAMES;

    public static void main(String[] args) {
        // Setup ID and Peers
        MY_ID = Integer.parseInt(System.getenv().getOrDefault("NODE_ID", "1"));
        String peersEnv = System.getenv().getOrDefault("PEERS", "");
        PEER_HOSTNAMES = peersEnv.isEmpty() ? List.of() : Arrays.asList(peersEnv.split(","));

        System.out.println("--- NODE " + MY_ID + " STARTED ---");

        // Start Server (So others can ping me)
        Javalin app = Javalin.create().start(7000);
        app.get("/ping", ctx -> ctx.result("Pong from " + MY_ID));
        // exposing /ping endpoint

        // Start Background Tasks
        new Thread(Node::runFailureDetector).start();
        new Thread(Node::runLeaderElection).start();
    }

    // TASK 1: Ping everyone to see who is alive
    private static void runFailureDetector() {
        while (true) {
            for (String peerHost : PEER_HOSTNAMES) {
                // peerHost looks like "node-2". We assume ID is the last char.
                // In real apps, we'd have a better discovery system.
                int peerId = Integer.parseInt(peerHost.split("-")[1]);

                if (pingPeer(peerHost)) {
                    // Update timestamp if ping success
                    peerLastSeen.put(peerId, System.currentTimeMillis());
                }
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
        }
    }

    // TASK 2: Decide who is the leader based on who is alive
    private static void runLeaderElection() {
        while (true) {
            // 1. Filter out dead nodes (haven't seen in 5 seconds)
            long now = System.currentTimeMillis();
            List<Integer> aliveNodes = new ArrayList<>();
            aliveNodes.add(MY_ID); // I am always alive to myself

            peerLastSeen.forEach((id, lastSeen) -> {
                if (now - lastSeen < 5000) { // 5 second timeout
                    aliveNodes.add(id);
                }
            });

            // 2. The Bully Algorithm: Max ID wins
            int leaderId = Collections.max(aliveNodes);

            String status = (leaderId == MY_ID) ? "I AM THE LEADER" : "Leader is Node " + leaderId;
            System.out.println("--- [Status] Alive: " + aliveNodes + " | " + status + " ---");

            try { Thread.sleep(2000); } catch (InterruptedException e) {}
        }
    }

    private static boolean pingPeer(String host) {
        Request request = new Request.Builder().url("http://" + host + ":7000/ping").build();
        try (Response response = client.newCall(request).execute()) {
            return response.isSuccessful();
        } catch (Exception e) {
            return false;
        }
    }
}