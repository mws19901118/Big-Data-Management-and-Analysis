package bigdata.assignment4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

public class nontrivalwords extends Configured implements Tool {

	  public static void main(String[] args) throws Exception {
		  int res = ToolRunner.run(new nontrivalwords(), args);
		  System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
		  Job job = Job.getInstance(getConf(), "nontrivalwords");
		  job.getConfiguration().setBoolean("nontrivalwords.skip.patterns", true);
		  job.setJarByClass(this.getClass());
		  // Use TextInputFormat, the default unless job.setInputFormatClass is used
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		  private final static IntWritable one = new IntWritable(1);
		  private boolean caseSensitive = false;
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
			  this.caseSensitive = config.getBoolean("nontrivalwords.case.sensitive", false);
			  if (config.getBoolean("nontrivalwords.skip.patterns", false)) {
				  parseSkipFile(new Path("hdfs://cshadoop1/user/jxw142530/stop_words.txt"));
			  }
		  }

		  private void parseSkipFile(Path path) {
			  try {
				  BufferedReader fis = new BufferedReader(new FileReader(new File(path.getName())));
				  String pattern;
				  while ((pattern = fis.readLine()) != null) {
					  patternsToSkip.add(pattern);
				  }
			  } catch (IOException ioe) {
				  System.err.println("Caught exception while parsing the cached file '" + path + "' : " + StringUtils.stringifyException(ioe));
			  }
		  }

		  public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			  String line = lineText.toString();
			  if (!caseSensitive) {
				  line = line.toLowerCase();
			  }
			  line=line.replaceAll("([^a-z])+", " ").trim();
			  Text currentWord = new Text();
	    	  for (String word : WORD_BOUNDARY.split(line)) {
		      	if (word.isEmpty() || patternsToSkip.contains(word)) {
		            continue;
		        }
		        currentWord = new Text(word);
		        context.write(currentWord,one);
		        }
	      }
	  }

	  public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		  @Override
		  public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			  for (IntWritable count : counts) {
			  }
			  context.write(word, new Text(""));
	      }
	  }
}
