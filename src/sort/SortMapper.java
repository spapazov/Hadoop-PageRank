package sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		
		// Identify difference 
		String[] data = value.toString().split("\t");
		Double difference = Double.parseDouble(data[1]);
		
		// Emit negative difference to ensure descending order 
		context.write(new DoubleWritable(-1 * difference), new Text(""));
	}

}
