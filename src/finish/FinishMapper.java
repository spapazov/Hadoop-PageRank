package finish;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String[] data = value.toString().split("\t");
		// Find node and rank
		String node = data[0];
		Double rank = Double.parseDouble(data[2]);
		rank = rank * -1;
		
		// Emit negative rank so that ordering occurs in decending order
		context.write(new DoubleWritable(rank), new Text(node));
	}
}
