/*Requirement - 
 * NYSE is a stock exchange data file with following fields as data. 
Exchange Name, 
Stock ID 
Date 
Open 
high 
low 
close 
vol 
adj close. 
Requirement - Find the all time high for a particular Stock ID. 
 */
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;


public class StockATH {

	/**
	 * @param args
	 */
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] str = value.toString().split(",");
			Double ATH = Double.parseDouble(str[4]);
			context.write(new Text(str[1]), new DoubleWritable(ATH));
			
		}
		
		
	}
	
	public static class ReduceClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			
			Double ATH = 0.0; 
			for (DoubleWritable val:values){
				if (val.get() > ATH){
					ATH = val.get();
					
				}
			}
			context.write(new Text(key), new DoubleWritable(ATH));
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ATH Calc");
		job.setJarByClass(StockATH.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}

}
