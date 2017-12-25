package co.uk.khalidmammadov.hadoop;


import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool{

  public int run(String[] args) throws Exception {
	  
	    
	    Job job = Job.getInstance(getConf(), "reverse index builder");
	    
	    job.setJarByClass(Main.class);
	    job.setMapperClass(ParserMap.class);
	    job.setReducerClass(ReverseIndexReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    Path inputFilePath = new Path(args[0]);
	    Path outputFilePath = new Path(args[1]);		
	    FileInputFormat.addInputPath(job, inputFilePath);
	    FileOutputFormat.setOutputPath(job, outputFilePath);
	    
	    //For local run
	    //Path inputFilePath = new Path("/home/khalid/workspace/hadoop_reverse_index_mapreduce/input");
	    //Path outputFilePath = new Path("/home/khalid/workspace/hadoop_reverse_index_mapreduce/output/1");	    
	    //FileInputFormat.addInputPath(job, inputFilePath);
	    //FileOutputFormat.setOutputPath(job, outputFilePath);
	    
	    FileSystem  fs = FileSystem.get(new URI(outputFilePath.toString()), getConf());
	    fs.delete(outputFilePath, true);
	    
	   return job.waitForCompletion(true) ? 0 : 1;
	    
	  } 

  public static void main(String[] args) throws Exception {
	  	  
	  int exitCode = ToolRunner.run(new Main(), args);
	  System.exit(exitCode);
  }
  
}
