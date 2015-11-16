package bigdata.tfidf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

public class tfidf extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new tfidf(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		  Job job = Job.getInstance(getConf(), "tfidf");
		  job.getConfiguration().setBoolean("tfidf.skip.patterns", true);
		  job.setJarByClass(this.getClass());
		  // Use TextInputFormat, the default unless job.setInputFormatClass is used
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(DoubleWritable.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(DoubleWritable.class);
		  return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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
			  parseSkipFile(new Path("hdfs://cshadoop1/user/jxw142530/stop_words.txt"));
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
			  HashMap<String, Integer> termcount = new HashMap<String, Integer>();
			  int wordcount = 0;
			  String line = lineText.toString();
			  line=line.toLowerCase().replaceAll("([^a-z])+", " ").trim();
			  Text currentWord = new Text();
	    	  for (String word : WORD_BOUNDARY.split(line)) {
	    		  if (word.isEmpty() || patternsToSkip.contains(word)) {
	    			  continue;
	    		  }
	    		  wordcount += 1;
	    		  if (termcount.containsKey(word)) {
	    			  termcount.put(word, termcount.get(word) + 1);
	    		  } else {
	    			  termcount.put(word, 1);
	    		  }
		      }
	    	  context.write(new Text("#"), new DoubleWritable(1.0));
	    	  for (String key : termcount.keySet()) {
				  context.write(new Text(key), new DoubleWritable(termcount.get(key) * 1.0 / wordcount));
			  }
	      }
	  }

	  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		  private HashMap<String, Double> countfrequency = new HashMap<String, Double>();
		  private HashMap<String, Integer> countfile = new HashMap<String, Integer>();
		  private int totalfile = 0;
		  public void reduce(Text word, Iterable<DoubleWritable> frequencies, Context context) throws IOException, InterruptedException {
			  if (word.toString().equals("#")) {
				  for (DoubleWritable frequency : frequencies) {
					  totalfile += 1;
				  }
			  } else {
				  double sumfrequency = 0;
				  int sumfile = 0;
				  for (DoubleWritable frequency : frequencies) {
					  sumfrequency += frequency.get();
					  sumfile += 1;
				  }
				  countfrequency.put(word.toString(), sumfrequency);
				  countfile.put(word.toString(), sumfile);
			  }
	      }
		  
		  @Override
		  protected void cleanup(Context context) throws IOException, InterruptedException {
			  for (String key : countfile.keySet()) {
				  double t = Math.log(totalfile * 1.0 / countfile.get(key)) * countfrequency.get(key);
				  context.write(new Text(key), new DoubleWritable(t));
			  }
		  }
	  }

}
