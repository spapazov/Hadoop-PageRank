package recip;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecipMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		
		// Find Node and Neighbors
		
		String [] data = value.toString().split("\t");
		String node = data[0];
		String[] neighbors = data[1].split(",");
		for (String neigh: neighbors) {
			context.write(new Text(neigh), new Text("RECIP" + "\t" + node));
		}
		
		// Emit neighbors to node
		context.write(new Text(node), new Text("NEIGH" + "\t" + data[1]));
	}
}
