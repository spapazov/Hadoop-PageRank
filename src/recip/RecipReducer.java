package recip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RecipReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		
		String node = key.toString();
		ArrayList<String> edges = new ArrayList<String>();
		TreeSet<String> neighbors = new TreeSet<String>();
		
		for (Text value: values) {
			String[] data = value.toString().split("\t");
			if (data[0].equals("NEIGH")) {
				String [] nodes = data[1].split(",");
				for (String n: nodes) {
					neighbors.add(n);
				}
			} else {
				edges.add(data[1]);
			}
		}
		
		for (String edge: edges) {
			if (edge.compareTo(node) < 0 && neighbors.contains(edge)) {
				context.write(new Text("FOUND"), new Text(edge + "\t" + node));
			}
		}
		
	}
}
