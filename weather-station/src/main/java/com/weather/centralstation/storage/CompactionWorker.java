package com.weather.centralstation.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CompactionWorker implements Runnable {
    private final String directory;
    private final Map<String, KeyDirEntry> keyDir;

    public CompactionWorker(String directory, Map<String, KeyDirEntry> keyDir) {
        this.directory = directory;
        this.keyDir = keyDir;
    }

    @Override
    public void run() {
        while (true) {
            try {
                // Wake up every 60 seconds to compact files
                Thread.sleep(60000);
                System.out.println("[CompactionWorker] Waking up to merge old segment files...");
                compact();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void compact() {
        try {
            // Identify which files to compact
            File dir = new File(directory);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".data") && !name.startsWith("compacted_"));

            if (files == null || files.length <= 1) {
                return; // Nothing to compact (only the active file exists)
            }

            // Sort files by timestamp (oldest first)
            List<File> fileList = Arrays.asList(files);
            fileList.sort(Comparator.comparing(File::getName));

            // Exclude the absolute newest file (the one Kafka is currently writing to)
            List<File> filesToCompact = fileList.subList(0, fileList.size() - 1);
            if (filesToCompact.isEmpty())
                return;

            // Setup the new compacted file and hint file
            String compactedFileId = "compacted_" + System.currentTimeMillis();
            File compactedFile = new File(directory, compactedFileId + ".data");
            File hintFile = new File(directory, compactedFileId + ".hint");

            try (RandomAccessFile dataOut = new RandomAccessFile(compactedFile, "rw");
                    RandomAccessFile hintOut = new RandomAccessFile(hintFile, "rw")) {

                long currentWriteOffset = 0;

                // Read through all the old files
                for (File oldFile : filesToCompact) {
                    String oldFileId = oldFile.getName().replace(".data", "");

                    try (RandomAccessFile reader = new RandomAccessFile(oldFile, "r")) {
                        long fileLength = reader.length();

                        while (reader.getFilePointer() < fileLength) {

                            // Extract Key
                            int keyLen = reader.readInt();
                            byte[] keyBytes = new byte[keyLen];
                            reader.readFully(keyBytes);
                            String key = new String(keyBytes, StandardCharsets.UTF_8);

                            // Extract Value
                            int valLen = reader.readInt();
                            long oldValOffset = reader.getFilePointer(); // Save the exact byte offset
                            byte[] valBytes = new byte[valLen];
                            reader.readFully(valBytes);

                            // Verify if this record is still valid
                            KeyDirEntry currentEntry = keyDir.get(key);

                            if (currentEntry != null &&
                                    currentEntry.getFileId().equals(oldFileId) &&
                                    currentEntry.getValueOffset() == oldValOffset) {

                                // Write to the new compacted .data file
                                dataOut.writeInt(keyLen);
                                dataOut.write(keyBytes);
                                dataOut.writeInt(valLen);
                                dataOut.write(valBytes);

                                long newValOffset = currentWriteOffset + 4 + keyLen + 4;
                                currentWriteOffset += (4 + keyLen + 4 + valLen);

                                // Write to the .hint file
                                hintOut.writeInt(keyLen);
                                hintOut.write(keyBytes);
                                hintOut.writeInt(valLen);
                                hintOut.writeLong(newValOffset);
                                hintOut.writeLong(currentEntry.getTimestamp());

                                // Safely update the KeyDir pointer (if we wrote to a new compacted file)
                                KeyDirEntry newEntry = new KeyDirEntry(compactedFileId, valLen, newValOffset,
                                        currentEntry.getTimestamp());
                                keyDir.replace(key, currentEntry, newEntry);
                            }
                        }
                    }
                    // Delete the old segment file
                    oldFile.delete();
                }
            }
            System.out.println("[CompactionWorker] Compaction finished. Created: " + compactedFileId);
        } catch (IOException e) {
            System.err.println("[CompactionWorker] Error during compaction: " + e.getMessage());
        }
    }
}