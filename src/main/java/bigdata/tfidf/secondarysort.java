package bigdata.tfidf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.tfidf.tfidf.Map;
import bigdata.tfidf.tfidf.Reduce;

public class secondarysort extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new secondarysort(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		  Job job = Job.getInstance(getConf(), "secondarysort");
		  job.getConfiguration().setBoolean("nontrivalwords.skip.patterns", true);
		  job.setJarByClass(this.getClass());
		  // Use TextInputFormat, the default unless job.setInputFormatClass is used
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setMapOutputKeyClass(DoubleWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(DoubleWritable.class);
		  job.setOutputValueClass(Text.class);
		  return job.waitForCompletion(true) ? 0 : 1;
	  }
	
	public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		  private String input;
		  private Set<String> patternsToSkip = new HashSet<String>();
		  private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		  
		  protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			  if (context.getInputSplit() instanceof FileSplit) {
				  this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			  } else {
				  this.input = context.getInputSplit().toString();
			  }
			  Configuration config = context.getConfiguration();
		  }

		  public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			  String line = lineText.toString();
	    	  String[] s = WORD_BOUNDARY.split(line);
	    	  context.write(new DoubleWritable(Double.parseDouble(s[1])), new Text(s[0]));
		      }
	      }
		  
		  public static class Reduce extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
			  public void reduce(DoubleWritable a, Iterable<Text> b, Context context) throws IOException, InterruptedException {
				  for (Text s : b) {
					  context.write(a, s);
				  }
		      }
		  }
	  }

