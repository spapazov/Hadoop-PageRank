package driver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import Iter.IterMapper;
import Iter.IterReducer;
import diff.DiffMapper;
import diff.DiffReducer;
import finish.FinishMapper;
import finish.FinishReducer;
import init.InitMapper;
import init.InitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ratio.RatioMapper;
import ratio.RatioReducer;
import recip.RecipMapper;
import recip.RecipReducer;
import sort.SortMapper;
import sort.SortReducer;


public class SocialRankDriver
{
  public static void main(String[] args) throws Exception 
  {
    if (args[0].equals("init")) { // execute init code
    	boolean jobCompleated = init(args);
    	System.exit(jobCompleated ? 1 : 0);
    } else if (args[0].equals("iter")) { // execute iter code
    	boolean jobCompleated = iter(args);
    	System.exit(jobCompleated ? 1 : 0);
    } else if (args[0].equals("diff")) { // execute diff 
    	boolean jobCompleated = diff(args);
    	System.exit(jobCompleated ? 1 : 0);
    } else if (args[0].equals("finish")) { // execute finish
    	boolean jobCompleated = finish(args);
    	System.exit(jobCompleated ? 1 : 0);
    } else if (args[0].equals("composite")) { // execute composite
    	if (args.length != 7) {
    		System.exit(-1);
    	}
    	
    	// run init and check if compleated 
    	boolean initCompleated = init(new String[] {"init", args[1], args[3], args[6]});
    	if (!initCompleated) System.exit(-1);
    	
    	// set up iter global variables
    	final double convergentDifference = 30;
    	double prevDifference = Double.MAX_VALUE;
    	int diffCounter = 1;
    	boolean activateDirectory1 = true;
    	
    	// loop and alternate between running iter and diff
    	while (prevDifference > convergentDifference) {
    		if (diffCounter % 3 == 0) {
    			boolean diffCompleated = diff(new String[] {"diff", args[3], args[4], args[5], args[6]});
    			if (!diffCompleated) System.exit(-1);
    			prevDifference = readDiffResult(args[5]);
    			diffCounter++;
    		} else {
    			if (activateDirectory1) {
    				boolean iterCompleated = iter(new String[] {"iter", args[3], args[4], args[6]});
    				if (!iterCompleated) System.exit(-1);
    				activateDirectory1 = !activateDirectory1;
    				diffCounter++;
    			} else {
    				boolean iterCompleated = iter(new String[] {"iter", args[4], args[3], args[6]});
    				if (!iterCompleated) System.exit(-1);
    				activateDirectory1 = !activateDirectory1;
    				diffCounter++;
    			}
    		}
    	}
    	
    	// finish job depending on end directory of iter
    	if (activateDirectory1) {
    		boolean finishCompleated = finish(new String[] {"finish", args[3], args[2], args[6]});
    		if (!finishCompleated) System.exit(-1);
    	} else {
    		boolean finishCompleated = finish(new String[] {"finish", args[4], args[2], args[6]});
    		if (!finishCompleated) System.exit(-1);
    	}
    	
    } else if (args[0].equals("recip")) {
    	boolean jobCompleated = recip(args);
    	System.exit(jobCompleated ? 1 : 0);
    } else { // invalid arg[0] given
    	System.exit(-1);
    }
  }
  
  static boolean init(String[] args) throws Exception {
	  System.out.println("Stefan Papazov: spapazov");
		
	    if (args.length != 4) {
	      System.err.println("Invalid Arguments");
	      System.exit(-1);
	    }
	    
	    
	    Configuration config = new Configuration();
	    Job job = Job.getInstance(config);
	    
	    //Configure job
	    
	    job.setJarByClass(SocialRankDriver.class);
	    job.setMapperClass(InitMapper.class);
	    job.setReducerClass(InitReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(Integer.parseInt(args[3]));
	    
	    //Set the paths to input and output 
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    deleteDirectory(args[2]);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    //Wait to finish job
	    return job.waitForCompletion(true);
	    
  }
  
  static boolean iter(String args[]) throws Exception {
	  System.out.println("Stefan Papazov: spapazov");
		
	    if (args.length != 4) {
	      System.err.println("Invalid Arguments");
	      System.exit(-1);
	    }
	    
	    
	    Configuration config = new Configuration();
	    Job job = Job.getInstance(config);
	    
	    //Configure job
	    
	    job.setJarByClass(SocialRankDriver.class);
	    job.setMapperClass(IterMapper.class);
	    job.setReducerClass(IterReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(Integer.parseInt(args[3]));
	    
	    //Set the paths to input and output 
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    deleteDirectory(args[2]);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    //Wait to finish job
	    return job.waitForCompletion(true);
  }
  
  static boolean diff(String[] args) throws Exception {
		
	    if (args.length != 5) {
	      System.err.println("Invalid Arguments");
	      System.exit(-1);
	    }
	    
	    
	    Configuration config = new Configuration();
	    Job job = Job.getInstance(config);
	    
	    //Configure job
	    
	    job.setJarByClass(SocialRankDriver.class);
	    job.setMapperClass(DiffMapper.class);
	    job.setReducerClass(DiffReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(Integer.parseInt(args[4]));
	    
	    //Set the paths to input and output 
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileInputFormat.addInputPath(job, new Path(args[2]));
	    deleteDirectory("temp");
	    FileOutputFormat.setOutputPath(job, new Path("temp"));
	    
	    //Wait to finish job
	    job.waitForCompletion(true);
	    
	    // Start Second Part of Diff job (finding max diff)
	    Configuration configFindMax = new Configuration();
	    Job jobMax = Job.getInstance(configFindMax);
	    
	    //Configure job
	    
	    jobMax.setJarByClass(SocialRankDriver.class);
	    jobMax.setMapperClass(SortMapper.class);
	    jobMax.setReducerClass(SortReducer.class);
	    jobMax.setMapOutputKeyClass(DoubleWritable.class);
	    jobMax.setMapOutputValueClass(Text.class);
	    jobMax.setOutputKeyClass(Text.class);
	    jobMax.setOutputValueClass(Text.class);
	    jobMax.setNumReduceTasks(1);
	    
	    //Set the paths to input and output 
	    FileInputFormat.addInputPath(jobMax, new Path("temp"));
	    deleteDirectory(args[3]);
	    FileOutputFormat.setOutputPath(jobMax, new Path(args[3]));
	    
	    //Wait to finish job
	    boolean jobCompleated = jobMax.waitForCompletion(true);
	    deleteDirectory("temp");
	    return jobCompleated;
  }
  
  static boolean finish(String[] args) throws Exception {
	  System.out.println("Stefan Papazov: spapazov");
		
	    if (args.length != 4) {
	      System.err.println("Invalid Arguments");
	      System.exit(-1);
	    }
	    
	    
	    Configuration config = new Configuration();
	    Job job = Job.getInstance(config);
	    
	    //Configure job
	    
	    job.setJarByClass(SocialRankDriver.class);
	    job.setMapperClass(FinishMapper.class);
	    job.setReducerClass(FinishReducer.class);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(Integer.parseInt(args[3]));
	    
	    //Set the paths to input and output 
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    deleteDirectory(args[2]);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    //Wait to finish job
	    return job.waitForCompletion(true);
  }
  
  static boolean recip(String[] args) throws Exception {
	  
	// run init and check if compleated 
  	boolean initCompleated = init(new String[] {"init", args[1], "temp", args[3]});
  	if (!initCompleated) System.exit(-1);
  	
  	Configuration config = new Configuration();
    Job job = Job.getInstance(config);
    
    //Configure job
    
    job.setJarByClass(SocialRankDriver.class);
    job.setMapperClass(RecipMapper.class);
    job.setReducerClass(RecipReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(Integer.parseInt(args[3]));
    
    //Set the paths to input and output 
    FileInputFormat.addInputPath(job, new Path("temp"));
    deleteDirectory("edges12");
    FileOutputFormat.setOutputPath(job, new Path("edges12"));
    
    //Wait to finish job
    boolean jobCompleated = job.waitForCompletion(true);
    if (!jobCompleated) System.exit(-1);
    
 // Start Second Part of job (calculating ratio)
    Configuration configFindRatio = new Configuration();
    Job jobMax = Job.getInstance(configFindRatio);
    
    //Configure job
    
    jobMax.setJarByClass(SocialRankDriver.class);
    jobMax.setMapperClass(RatioMapper.class);
    jobMax.setReducerClass(RatioReducer.class);
    jobMax.setMapOutputKeyClass(Text.class);
    jobMax.setMapOutputValueClass(Text.class);
    jobMax.setOutputKeyClass(Text.class);
    jobMax.setOutputValueClass(Text.class);
    jobMax.setNumReduceTasks(1);
    
    //Set the paths to input and output 
    FileInputFormat.addInputPath(jobMax, new Path("edges12"));
    FileInputFormat.addInputPath(jobMax, new Path(args[1]));
    deleteDirectory(args[2]);
    FileOutputFormat.setOutputPath(jobMax, new Path(args[2]));
    
    //Wait to finish job
    boolean jobCompleated2 = jobMax.waitForCompletion(true);
    deleteDirectory("temp");
    deleteDirectory("edges12");
    return jobCompleated2;
  }

  // Given an output folder, returns the first double from the first part-r-00000 file
  static double readDiffResult(String path) throws Exception 
  {
    double diffnum = 0.0;
    Path diffpath = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(diffpath)) {
      FileStatus[] ls = fs.listStatus(diffpath);
      for (FileStatus file : ls) {
	if (file.getPath().getName().startsWith("part-r-00000")) {
	  FSDataInputStream diffin = fs.open(file.getPath());
	  BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
	  String diffcontent = d.readLine();
	  diffnum = Double.parseDouble(diffcontent);
	  d.close();
	}
      }
    }
    
    fs.close();
    return diffnum;
  }

  static void deleteDirectory(String path) throws Exception {
    Path todelete = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(todelete)) 
      fs.delete(todelete, true);
      
    fs.close();
  }

}
