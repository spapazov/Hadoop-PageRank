package diff;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DiffReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		
		String val1 = null;
		String val2 = null;
		int counter = 1;
		
		// Find initial rank and final rank
		for (Text value: values) {
			if (counter == 1) {
				val1 = value.toString();
				counter++;
			} else {
				val2 = value.toString();
				counter++;
			}
		}
		
		// Find difference and emit to file
		Double difference = Double.parseDouble(val1) - Double.parseDouble(val2);
		difference = Math.abs(difference);
		context.write(key, new Text("" + difference));
	}
}
