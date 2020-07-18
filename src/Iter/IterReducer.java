package Iter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {

		String node = key.toString();
		String neighbors = "";
		Double rank = 0.0;
		
		// Iterate through values
		// If values is an incoming rank add it to total rank
		// If values identifies neighbors set neighbors 
		for (Text value: values) {
			String[] data = value.toString().split(" ");
			if (data[0].equals("RANK")) {
				Double rankToAdd = Double.parseDouble(data[1]);
				rank+= rankToAdd;
			} else {
				neighbors = data[1];
			}
		}
		
		// Adjust rank with constants
		double rankToWrite = 0.15 + (0.85 * rank);
		
		// Write node, neighbors and new rank
		context.write(new Text(node), new Text(neighbors + "\t" + rankToWrite));
	}
}
