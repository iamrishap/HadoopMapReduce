package comp9313.rishap;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class WordStartCount {
	public static class WordTokenizer extends Mapper<Object, Text, Text, IntWritable>{
		public static IntWritable one = new IntWritable(1);
		public static Character c;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer inputWords = new StringTokenizer(value.toString());
			while(inputWords.hasMoreTokens()){
				c = inputWords.nextToken().charAt(0);
				c = Character.toLowerCase(c);
				if(Character.isLetter(c)){
					context.write(new Text(c.toString()), one);
				}
			}
		}
		
	}
	
	public static class WordStartCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException{
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
		Job job = Job.getInstance(conf, "word start count");
		job.setJarByClass(WordStartCount.class);
	    job.setMapperClass(WordTokenizer.class);
	    job.setReducerClass(WordStartCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
