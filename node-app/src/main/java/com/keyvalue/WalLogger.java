package com.keyvalue;


import java.io.*;
import java.util.Map;

public class WalLogger {
    private final File walFile;
    private final BufferedWriter writer;

    public WalLogger(int nodeId) throws IOException {
        // Ensure directory exists inside the container
        File dir = new File("/app/data");
        if (!dir.exists()) dir.mkdirs();

        // The specific log file for this node
        this.walFile = new File(dir, "node_" + nodeId + ".wal");

        // Append mode = true (Don't overwrite, just add to end)
        this.writer = new BufferedWriter(new FileWriter(walFile, true));
    }

    // 1. WRITE: Append command to disk
    public synchronized void write(String key, String value) {
        try {
            writer.write(key + "," + value);
            writer.newLine();
            writer.flush(); // Force write to physical disk immediately
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Disk Write Failed! Database stopped.");
        }
    }

    // 2. RECOVERY: Read file line by line and fill the HashMap
    public void replay(Map<String, String> kvStore) {
        if (!walFile.exists()) return;

        System.out.println("--- [Recovery] Replaying WAL from disk... ---");
        try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    kvStore.put(parts[0], parts[1]);
                    count++;
                }
            }
            System.out.println("--- [Recovery] Restored " + count + " entries from disk ---");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}