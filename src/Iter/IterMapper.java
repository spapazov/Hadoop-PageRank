package Iter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper <LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		
		String[] data = value.toString().split("\t");
		
		try {
			// Identify rank and nieghbors 
			Double rank = Double.parseDouble(data[2]);
			String[] neighbors = data[1].split(",");
			
			// Calculate rank to emit 
			Integer numNeighbors = neighbors.length;
			Double emitRank = rank * (1.0 / numNeighbors);
			
			// If node has neighbors, emit to neighbors 
			if (!data[1].equals("NONE")) {
				for (String neighbor: neighbors) {
					context.write(new Text(neighbor), new Text("RANK" + " " + emitRank));
				}
			} 
			
			// Write to the current node its neighbors (in order to keep track)
			context.write(new Text(data[0]), new Text("NEIGHBORS" + " " + data[1]));
		} catch (Exception e) {
			System.exit(-1);
		}
	}
	
}
