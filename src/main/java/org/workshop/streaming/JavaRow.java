package org.workshop.streaming;

/** Java Bean class for converting RDD to DataFrame */
public class JavaRow implements java.io.Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}