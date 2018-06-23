package comp9313.rishap;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * The problem is to compute the number of co-occurrence for each pair of 
 * terms (w, u) in the document. In this problem, the co-occurrence of (w, u) is 
 * defined as: u appears after w in a line of document. This means that, the co-
 * occurrence counts of (w, u) and (u, w) are different
 * 
 * Input is in format of (line number, line). Output is in format of ((w, u), co-occurrence count). 
 */

// TODO: Make a custom WritableComparator for the Pair

public class PairCooccurence {
	
	public static class WordPairTokenizer extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer inputWords = new StringTokenizer(value.toString(), " *$&#“—’/\t\n\f\"'\\,.:;?![](){}<>~-_");
			ArrayList<String> wordsInLine = new ArrayList<String>();
			String word1 = null;
			String word2 = null;
			while(inputWords.hasMoreTokens()){
				wordsInLine.add(inputWords.nextToken().toLowerCase());
			}
			for(int i = 0; i < wordsInLine.size(); i++){
				word1 = wordsInLine.get(i);
				for(int j = i + 1; j < wordsInLine.size(); j++){
					word2 = wordsInLine.get(j);
					context.write(new Text(word1 + " " + word2), one);
				}
			}
		}
	}
	
	public static class WordPairCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			for(IntWritable value: values){
				count += value.get();
			}
			result.set(count);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "pair co-occurance");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(WordPairTokenizer.class);
	    job.setReducerClass(WordPairCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
