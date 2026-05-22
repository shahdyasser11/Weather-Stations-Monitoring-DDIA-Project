package com.weather.centralstation.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class SegmentManager {
    private final String directory;
    private String activeFileId;
    private RandomAccessFile activeWriter;
    private long currentOffset;

    // Rotate to a new file after 1MB
    private static final long MAX_FILE_SIZE = 1024 * 1024;

    public SegmentManager(String directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(Paths.get(directory));
        rotateActiveFile();
    }

    public synchronized void rotateActiveFile() throws IOException {
        if (activeWriter != null) {
            activeWriter.close();
        }
        activeFileId = String.valueOf(System.currentTimeMillis());
        File file = new File(directory, activeFileId + ".data");
        activeWriter = new RandomAccessFile(file, "rw");
        currentOffset = 0;
    }

    public synchronized KeyDirEntry append(String key, String value, long timestamp) throws IOException {
        if (currentOffset >= MAX_FILE_SIZE) {
            rotateActiveFile();
        }

        // conversion of data from sstring to bytes to be written in files
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        long startOffset = currentOffset;

        // Write Key Length, Key, Value Length, Value
        // so when reading we know how may bytes we are going to read
        activeWriter.writeInt(keyBytes.length);
        activeWriter.write(keyBytes);
        activeWriter.writeInt(valueBytes.length);
        activeWriter.write(valueBytes);

        // updating the offset
        int sizeWritten = 4 + keyBytes.length + 4 + valueBytes.length;
        currentOffset += sizeWritten;

        // Return the exact metadata needed to find this value later
        // the value offset is pointing to the exact location in file where the stream
        // of actual data is started

        return new KeyDirEntry(activeFileId, valueBytes.length, startOffset + 4 + keyBytes.length + 4, timestamp);
    }

    public String read(KeyDirEntry entry) throws IOException {
        File file = new File(directory, entry.getFileId() + ".data");
        try (RandomAccessFile reader = new RandomAccessFile(file, "r")) {
            reader.seek(entry.getValueOffset());
            byte[] valueBytes = new byte[entry.getValueSize()];
            reader.readFully(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        }
    }
}