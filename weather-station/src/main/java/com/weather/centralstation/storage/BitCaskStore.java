package com.weather.centralstation.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BitCaskStore {
    private final Map<String, KeyDirEntry> keyDir;
    private final SegmentManager segmentManager;
    private final CompactionWorker compactionWorker;

    public BitCaskStore(String directory) throws IOException {
        // to allow multiple codes to acess the same instance of the hash map
        this.keyDir = new ConcurrentHashMap<>();
        this.segmentManager = new SegmentManager(directory);

        // Start the background compaction thread
        this.compactionWorker = new CompactionWorker(directory, keyDir);
        Thread compactionThread = new Thread(this.compactionWorker);
        compactionThread.setDaemon(true); // setting the compaction thread as background thread to be terminated when
                                          // main is terminated
        compactionThread.start();
    }

    public synchronized void put(String key, String value) {
        try {
            long timestamp = System.currentTimeMillis();
            KeyDirEntry entry = segmentManager.append(key, value, timestamp);
            keyDir.put(key, entry);
        } catch (IOException e) {
            System.err.println("Failed to write to BitCask disk: " + e.getMessage());
        }
    }

    public String get(String key) {
        KeyDirEntry entry = keyDir.get(key);
        if (entry == null) {
            return null; // Station hasn't sent data yet
        }
        try {
            return segmentManager.read(entry);
        } catch (IOException e) {
            System.err.println("Failed to read from BitCask disk: " + e.getMessage());
            return null;
        }
    }

    // to get all our current hash map entries
    public Map<String, String> getAll() {
        Map<String, String> allData = new HashMap<>();
        // Iterate through the in-memory keys
        for (String key : keyDir.keySet()) {
            String value = get(key);
            if (value != null) {
                allData.put(key, value);
            }
        }
        return allData;
    }
}
