
/*
 * WordCountMapper.java
 *
 * Created on Oct 22, 2012, 5:31:10 PM
 */

package org.sample;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author mac
 */
public class TwoHopConnMapper extends Mapper<LongWritable, Text, Text, Text> {
	// The Karmasphere Studio Workflow Log displays logging from Apache Commons
	// Logging, for example:
	// private static final Log LOG =
	// LogFactory.getLog("org.sample.WordCountMapper");

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		HashMap<String, String[]> aMap = new HashMap<String, String[]>();
		
		// map process analyze input file line by line
		String aline = value.toString();
		String[] parts = aline.split(":");
		// take current node as key
		String mapKey = parts[0];
		String[] neighbors = parts[1].split(",");
		aMap.put(mapKey, neighbors);

		for (String node : neighbors) {
			context.write(new Text(mapKey), new Text(node));// neighbors of mapKey node
			for (String sink : neighbors) {
				if (node.equals(sink)){
					continue;
				}
				context.write(new Text(node), new Text(mapKey + "-" + sink));// neighbor's two hop connectivity node
			}
		}
	}
}
