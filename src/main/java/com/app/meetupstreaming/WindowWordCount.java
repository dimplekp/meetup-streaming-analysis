package com.app.meetupstreaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

// Computes word frequency counts per window and emits them at each endWindow. The output is a
// list of pairs (word, frequency).
//
public class WindowWordCount extends BaseOperator {
	private static final Logger LOG = LoggerFactory.getLogger(WindowWordCount.class);

	// wordMap : word => frequency
	protected Map<String, WCPair> wordMap = new HashMap<>();

	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String word) {
			WCPair pair = wordMap.get(word);
			if (null != pair) { // word seen previously
				pair.freq += 1;
				return;
			}

			// new word
			pair = new WCPair();
			pair.word = word;
			pair.freq = 1;
			wordMap.put(word, pair);
		}
	};

	// output port which emits the list of word frequencies for current window
	// fileName => list of (word, freq) pairs
	//
	public final transient DefaultOutputPort<List<WCPair>> output = new DefaultOutputPort<>();

	@Override
	public void endWindow() {
		LOG.info("WindowWordCount: endWindow");

		// got EOF; if no words found, do nothing
		if (wordMap.isEmpty())
			return;

		// have some words; emit single map and reset for next file
		final ArrayList<WCPair> list = new ArrayList<>(wordMap.values());
		output.emit(list);
		list.clear();
		wordMap.clear();
	}
}
