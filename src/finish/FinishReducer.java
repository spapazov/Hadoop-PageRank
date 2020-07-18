package finish;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {
		
		// Find rank difference and reverse sign
		Double rank = key.get();
		rank = rank * -1;
		
		// Write rank difference for each node
		for (Text value: values) {
			context.write(value, new Text("" + rank));
		}
	}
}
