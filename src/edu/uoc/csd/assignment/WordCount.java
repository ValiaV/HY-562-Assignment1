package edu.uoc.csd.assignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job1 = Job.getInstance(getConf(), "wordcount");
    job1.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    if (!job1.waitForCompletion(true)){
    	System.exit(1);
  	}
    
    Job job2 = Job.getInstance(getConf(), "sort by frequency");
	job2.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job2, new Path(args[1]+ "/temp"));
	FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
	job2.setMapperClass(Map2.class);
	job2.setReducerClass(Reduce2.class);
	job2.setOutputKeyClass(IntWritable.class);
	job2.setOutputValueClass(Text.class);
	job2.setSortComparatorClass(DescendingComparator.class);
	return job2.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	String line = lineText.toString();
    	Text currentSubword = new Text();
    	for (String word : WORD_BOUNDARY.split(line)) {

	    	word = word.toLowerCase();
	    	word = word.replaceAll("'","");   //remove single quotes
	    	word = word.replaceAll("[^a-zA-Z]"," ");  //remove the rest with a space
	    	word = word.trim(); //trim whitespaces
	
	    	//count subwords too
	    	for (String subword : WORD_BOUNDARY.split(word)){
		        if (subword.isEmpty()) {
		            continue;
		        }	
		        currentSubword = new Text(subword);
	            context.write(currentSubword,one);        
	    	}
	    	
	    }
    }
  }
  
  public static class Map2 extends Mapper<LongWritable,  Text, IntWritable, Text> {
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	    	
	    	Text word = new Text();
	    	IntWritable freq = new IntWritable();
	    	int key = 0;
	    	String value = "";
	    	
	    	String line = lineText.toString();
	    	
	    	//find word and frequency in the input line
	    	int flag = 0;
	    	for (String t : WORD_BOUNDARY.split(line)) {
		    	if (flag == 0){
		    		if (t.isEmpty()) {
		    			continue;
		    		} else{
		    			word = new Text(t);
		    			value = t;
		    			flag = 1;
		        	}
		        } else{
		        	if (t.isEmpty()) {
		    			continue;
		    		} else{
		    			key = Integer.parseInt(t.toString());
		    			freq = new IntWritable(key);
		    			break;
		        	}
		        }
	    	}
	    	
	    	context.write(freq, word);
	    }
  }
  
  public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
  
  public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {
	  @Override
	  public void reduce(IntWritable frequency, Iterable<Text> words, Context context)
          throws IOException, InterruptedException {
		  
		  int cutDown = 4000;
		  for (Text word : words) {

			  if (frequency.get() > cutDown ){   //greater than 4000, omit all other pairs
	    		 context.write(frequency, word);
			  }
	     }
	  }
  }

  public static class DescendingComparator extends WritableComparator {
	    protected DescendingComparator() {
	        super(IntWritable.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable key1, WritableComparable key2) {
	        IntWritable freq1 = (IntWritable) key1;
	        IntWritable freq2 = (IntWritable) key2;          
	        return -1 * freq1.compareTo(freq2);
	    }
	}
  
 }
