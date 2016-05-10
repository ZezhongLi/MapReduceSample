
/*
 * WordCountReducer.java
 *
 * Created on Oct 22, 2012, 5:33:40 PM
 */

package org.sample;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

/**
 *
 * @author mac
 */
public class TwoHopConnReducer extends Reducer<Text, Text, Text, Text> {
	// The Karmasphere Studio Workflow Log displays logging from Apache Commons
	// Logging, for example:
	// private static final Log LOG =
	// LogFactory.getLog("org.sample.WordCountReducer");

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		HashSet<String> neighbors = new HashSet<String>();
		HashSet<String> paths = new HashSet<String>();
		for (Text val : values) {
			if (val.find("-") == -1) {
				neighbors.add(val.toString());
			} else {
				paths.add(val.toString());
			}
		}
		HashMap<String, Integer> pool = new HashMap<String, Integer>();
		for (String path : paths) {
			String destNode = path.split("-")[1];

			if (!neighbors.contains(destNode)) {
				if (!pool.containsKey(destNode)) {
					pool.put(destNode, 0);
				}
				pool.replace(destNode, pool.get(destNode) + 1);
			}
		}
		
		PrintWriter writer = new PrintWriter(new FileOutputStream(
			    new File("requiredformat.txt"), 
			    true /* append = true */)); 
		HashMap<String, Integer> sortedMapByVal = sortByValues(pool);
		for (String node : sortedMapByVal.keySet()) {
			String outStr = "[" + node + ":" + pool.get(node) + "]";
			System.out.print(outStr);
			writer.print(outStr);
			context.write(new Text("=>" + key), new Text(outStr));
		}
		System.out.println("=>" + key);
		writer.println("=>" + key);
		writer.close();
	}

	private static HashMap<String, Integer> sortByValues(HashMap<String, Integer> map) {
		List list = new LinkedList(map.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				if (((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue()) == 0) {
					if (((Comparable) ((Map.Entry) (o1)).getKey()).compareTo(((Map.Entry) (o2)).getKey()) < 0) {
						return -1;
					}
					return 1;
				} else {
					return -((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
				}
			}
		});

		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap<String, Integer> sortedHashMap = new LinkedHashMap<String, Integer>();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Integer> entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}
}
