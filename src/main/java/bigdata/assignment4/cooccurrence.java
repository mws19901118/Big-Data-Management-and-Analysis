package bigdata.assignment4;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class cooccurrence extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(cooccurrence.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new cooccurrence(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    Job job = Job.getInstance(getConf(), "cooccurrence");
    job.setJarByClass(this.getClass());
    job.setInputFormatClass(WholeFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  
  public static class Map extends Mapper<NullWritable, BytesWritable, Text, MapWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    protected void setup(Mapper.Context context)
	        throws IOException,
	        InterruptedException {
	    }
    
    public void map(NullWritable key, BytesWritable file, Context context)
        throws IOException, InterruptedException {
    	byte [] content = file.getBytes();
    	String wholefile = new String(content);
    	/*DocumentPreprocessor dp = new DocumentPreprocessor(wholefile);
    	for (List<HasWord> sentence : dp) {
    		Sentence.listToString(sentence);
    		context.write(new Text(Sentence.listToString(sentence)), one);
          }*/
    	Pattern SENTENCE_BOUNDARY = Pattern.compile("[!?.]\\s");
    	Pattern WORD_BOUNDARY = Pattern.compile("\\s+");
    	for (String sentence : SENTENCE_BOUNDARY.split(wholefile)) {						//Split the whole text to an array of sentences.
    		String processedsentence=sentence.toLowerCase().replaceAll("(\\W|\\d|_)+", " ").trim();	//Preprocess the sentence: convert all the characters to lower case, remove all the punctuation characters and trim spaces.
    		String[] words = WORD_BOUNDARY.split(processedsentence);						//Split the sentences to an array of words.
    		int l=words.length;
    		if ( l > 1 ) {
    			for (int i = 0 ; i < l ; i++ ) {											//Traverse through the array of words.
    				if ( words[i].isEmpty() ) {
    					continue;
    				}
    				MapWritable stripe = new MapWritable();
    				for (int j = i - 2; j <= i + 2; j++ ) {									//Traverse through the neighbors of current word(the max distance is 2).
    					if (j >= 0 && j != i && j < l) {									//The index of neighbor should be valid.
    						if ( stripe.containsKey(words[j]) ) {
    							IntWritable count = (IntWritable)stripe.get(new Text(words[j]));
    			                count.set(count.get()+1);
    						} else {
    							stripe.put(new Text(words[j]), one);
    						}
    					}
    				}
    				context.write(new Text(words[i] ), stripe);
    			}
    		}
    	}
    }
  }

  public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
    
	  private MapWritable incrementingMap = new MapWritable();
	  
	  protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	        }
	        context.write(key, new Text(convertToString(incrementingMap)));
	    }
	  
	  //Merge mapWritable.
	  private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
	  
	  //Print mapWritable.
	  private String convertToString(MapWritable mapWritable) {
		  Set<Writable> keys = mapWritable.keySet();
		  String s = new String("{ ");
		  for (Writable key : keys) {
			  IntWritable count = (IntWritable) mapWritable.get(key);
			  s = s + key.toString() + ":" + count.toString() + ", ";
		  }
		  s = s + " }";
		  return s;
	  }
  }
}
