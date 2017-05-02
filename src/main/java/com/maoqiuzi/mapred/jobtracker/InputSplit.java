package com.maoqiuzi.mapred.jobtracker;

//import tasktracker.SlaveStatus;

import java.io.Serializable;

/*
 * the input split to be send. we need the file path, name, start and end offset
 */
public class InputSplit implements Serializable {
    private int id;
    public String path = "path";
    public String name = "name";
    private long startOffset = 0;
    private long endOffset = 0;
    private final int MAX_ATTEMPT = 2;
    private int attempt;

    public InputSplit(int id, String path, String name, long start, long end) {
        this.id = id;
        this.path = path;
        this.name = name;
        this.startOffset = start;
        this.endOffset = end;
        this.attempt = 0;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }
    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public boolean attemptAllFail(){
        return this.attempt>=this.MAX_ATTEMPT;
    }

}
