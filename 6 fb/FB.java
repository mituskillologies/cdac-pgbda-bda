/* Design a distributed application using MapReduce which processes a log file of a system. List out the
users who have logged for maximum period on the system. Use simple log file from the Internet and
process it using a pseudo distribution mode on Hadoop platform. 

Programm by : Tushar B. Kute 
http://tusharkute.com
tushar@tusharkute.com */

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FB
{
	public static class FBMapper extends MapReduceBase implements
	Mapper <Object ,/*Input key Type */
	Text, /*Input value Type*/
	Text, /*Output key Type*/
	IntWritable> /*Output value Type*/
	{
		//Map function
		boolean flag = false;
		public void map(Object key, Text value,
		OutputCollector<Text, IntWritable> output,
		Reporter reporter) throws IOException
		{
			String line[] = value.toString().split(",",10);
			if(flag) {
				String type = line[1];
				String date = line[2];
				int likes = Integer.parseInt(line[6]);
				if(date.startsWith("2") && date.contains("2018") && type.equals("video"))
					output.collect(new Text("Likes"), new IntWritable(likes));
			}
			flag = true;
		}
	}
	//Reducer class
	public static class FBReducer extends MapReduceBase implements
	Reducer< Text, IntWritable, Text, IntWritable >
	{
		//Reduce function
		public void reduce(
		Text key,
		Iterator <IntWritable> values,
		OutputCollector<Text, IntWritable> output,
		Reporter reporter) throws IOException
		{
			int add = 0;
			while(values.hasNext())
					add = add + values.next().get();
					
			output.collect(key, new IntWritable(add));
		}
	}
	//Main function
	public static void main(String args[])throws Exception
	{
		JobConf conf = new JobConf(FB.class);
		conf.setJobName("Facebook Likes");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(FBMapper.class);
		conf.setReducerClass(FBReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
