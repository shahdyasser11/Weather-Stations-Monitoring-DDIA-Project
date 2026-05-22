package com.weather.centralstation.storage;

public class KeyDirEntry {
    private String fileId;
    private int valueSize;
    private long valueOffset;
    private long timestamp;

    public KeyDirEntry(String fileId, int valueSize, long valueOffset, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
        this.timestamp = timestamp;
    }

    public String getFileId() {
        return fileId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValueOffset() {
        return valueOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }
}