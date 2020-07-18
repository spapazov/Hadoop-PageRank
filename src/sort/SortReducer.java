package sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		
		// Get key
		double difference = key.get();
		
		// Remove negative sign 
		difference = difference * -1;
		
		// Write all nodes with that difference 
		for (Text value: values) {
			context.write(new Text("" + difference), new Text(""));
		}
	}
}
