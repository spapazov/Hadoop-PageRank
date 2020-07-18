package diff;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DiffMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		
		// Find node and rank
		String[] data = value.toString().split("\t");
		String node = data[0];
		String rank = data[2];
		
		// Emit rank to node
		context.write(new Text(node), new Text(rank));
	}
		
}
