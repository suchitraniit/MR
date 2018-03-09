import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.util.StringTokenizer;

public class WordC {

	/**
	 * @param args
	*/
	
	//Mapper Extension //Get the words and add 1 as a default count against each word
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			StringTokenizer str = new StringTokenizer(value.toString()," ");
			while (str.hasMoreElements()){
				String word = str.nextToken().toLowerCase();
				context.write(new Text(word), new IntWritable(1));
			}
		
			
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count=0;
			
			for (IntWritable val:values){
				count = count + val.get();
			}
			context.write(key, new IntWritable(count));
		
		}
	}
	
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Word Count");
		job.setJarByClass(WordC.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);

	}

}
