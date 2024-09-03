package edu.utexas.cs.cs378;

import java.io.Serializable;

public class SerializableObject implements Comparable<SerializableObject> {
    private final float fareAmount;
    private final String line;
    private int fileIndex;

    public SerializableObject() {
        this.fareAmount = 0;
        this.line = "";
        this.fileIndex = 0;
    }

    public SerializableObject(float fareAmount, String line) {
        this.fareAmount = fareAmount;
        this.line = line;
        this.fileIndex = 0;
    }

    public float getFareAmount() {
        return fareAmount;
    }

    public String getLine() {
        return line;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    @Override
    public int compareTo(SerializableObject o) {
        return Float.compare(fareAmount, o.fareAmount);
    }

    @Override
    public String toString() {
        return fareAmount + " " + line;
    }
}
