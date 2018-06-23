package comp9313.rishap;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountEfficient {
	
	public static class WordTokenizer extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private static HashMap<String, Integer> coutingHashMap = new HashMap<String, Integer>();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer inputWords = new StringTokenizer(value.toString()," *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			while(inputWords.hasMoreTokens()){
				String word = inputWords.nextToken().toLowerCase();
				if(coutingHashMap.containsKey(word)){
					coutingHashMap.put(word, coutingHashMap.get(word)+1);
				}else{
					coutingHashMap.put(word, 1);
				}
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			Set <Entry<String, Integer>> entries = coutingHashMap.entrySet();
			for(Entry<String, Integer> entry: entries){
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
	
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count efficient");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(WordTokenizer.class);
	    job.setReducerClass(WordCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
