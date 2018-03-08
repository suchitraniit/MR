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
Requirement - Find Total of stock volume for each stock ID. 
 */

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;


public class StockVol {

	/**
	 * @param args
	*/
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, Context context){
			try 
			{
				String str[] = value.toString().split(",");
				long vol = Long.parseLong(str[7]);
				context.write(new Text(str[1]), new LongWritable(vol));
				
			}catch (Exception e){
				
				System.out.println("Error with StockVol, Mapper Class Extension :" +e.getMessage());
			}
		}
	}
	
	
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			try{
				long sum = 0; 
				for (LongWritable val:values){
					sum += val.get();
				}
				context.write(new Text(key),new LongWritable(sum));
			}catch (Exception ex){
				System.out.println("Exception with the reduce class:" + ex.getMessage());
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Volume Count");
		job.setJarByClass(StockVol.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
