package com.keyvalue;

import io.javalin.Javalin;
import okhttp3.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Node {

    // Shared HTTP Client
    private static final OkHttpClient client = new OkHttpClient.Builder()
            .readTimeout(2, TimeUnit.SECONDS)
            .build();


    public static void main(String[] args) {
        // 1. Identity Configuration
        String myId = System.getenv().getOrDefault("NODE_ID", "Unknown");
        String peersEnv = System.getenv().getOrDefault("PEERS", "");
        List<String> peers = peersEnv.isEmpty() ? List.of() : Arrays.asList(peersEnv.split(","));
        int port = 7000;

        System.out.println("--- STARTING NODE " + myId + " ---");
        System.out.println("--- PEERS: " + peers + " ---");

        // 2. Start Server (Listener)
        Javalin app = Javalin.create().start(port);

        // Endpoint: Other nodes hit this to check if I am alive
        app.get("/ping", ctx -> {
            ctx.result("Pong from " + myId);
        });

        // 3. Start Background Broadcaster (Sender)
        // This thread runs forever, pinging peers every 2 seconds
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    for (String peer : peers) {
                        pingPeer(peer, myId);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void pingPeer(String peerHostname, String myId) {
        String url = "http://" + peerHostname + ":7000/ping";
        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                System.out.println("[" + myId + "] -> " + peerHostname + ": SUCCESS");
            } else {
                System.out.println("[" + myId + "] -> " + peerHostname + ": FAILED (Status " + response.code() + ")");
            }
        } catch (IOException e) {
            System.err.println("[" + myId + "] -> " + peerHostname + ": UNREACHABLE (Network Error)");
        }
    }



}