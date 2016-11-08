package com.app.meetupstreaming;

public class WCPair {
	  public String word;
	  public int freq;

	  public WCPair() {}

	  public WCPair(String w, int f) {
	    word = w;
	    freq = f;
	  }
	  
	  @Override
	  public String toString() {
	    return String.format("(%s, %d)", word, freq);
	  }
	}
