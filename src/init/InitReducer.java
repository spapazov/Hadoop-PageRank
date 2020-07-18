package init;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InitReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		
		// Create a string of neighbors
		String neighbors = "";
		for (Text data: values) {
			if (data.toString().equals("")) continue;
			neighbors += data.toString() + ",";
		}
		
		// If no neighbors emit node, NONE and rank 1 otherwise emit node, neighbors and rank 1 
		if (neighbors.length() >= 1) {
			context.write(key, new Text(neighbors + "\t" + "1"));
		} else {
			context.write(key, new Text("NONE" + "\t" + "1"));
		}
	}

}
