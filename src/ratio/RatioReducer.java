package ratio;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatioReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		double numberOfParallel = 0;
		double numberOfEdges = 0;
		
		// Iterate through vals and count bidirectional vs standard
		for (Text value: values) {
			String data = value.toString();
			if (data.equals("STANDARD")) {
				numberOfEdges++;
			} else {
				numberOfParallel++;
			}
		}
		
		// Calculate ratio 
		double ratio = numberOfParallel / numberOfEdges;
		
		// Write ratio 
		context.write(new Text("RATIO:"), new Text(" " + ratio));
	}
}
