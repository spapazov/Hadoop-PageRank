package init;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class InitMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
			
		String[] data = value.toString().split("\t");
		
		try {
			//Emit to node
			context.write(new Text(data[0]), new Text(data[1]));
			
			//Emit to neighbor (ensure 0 degree vertices discovered)
			context.write(new Text(data[1]), new Text(""));
		} catch (Exception e) {
			System.exit(-1);
		}
	}
}
