package ratio;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RatioMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String[] data = value.toString().split("\t");
		
		// Differentiate between parallel edges and standard input edges
		// Write to counting reducer
		if (data[0].equals("FOUND")) {
			context.write(new Text("COUNT"), new Text("PARALLEL"));
		} else {
			context.write(new Text("COUNT"), new Text("STANDARD"));
		}
	}
}
