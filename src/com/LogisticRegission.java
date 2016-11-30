package com;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class LogisticRegission {
	public static class DataMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public double getH(ArrayList<Double> W_list,ArrayList<Double> X_list)
		{
			double sum=0;
			for(int i=0;i<W_list.size();i++)
			{
				double m=W_list.get(i);
				double n=X_list.get(i);
				sum=sum+m*n;
			}
		//	return 1/(1+Math.exp(-1*sum));
			return sum;
		}
		ArrayList<Double> W_list=new ArrayList<Double>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration conf=context.getConfiguration() ;
			Path p=new Path(conf.get("HDFS"));
			 FileSystem fs = p.getFileSystem ( conf) ;
			  FSDataInputStream fsdis=fs.open(new Path(conf.get("model")));
			  for(int i=0;i<21;i++)
			  {
				  W_list.add(Double.valueOf(fsdis.readUTF()));
			  }
			  fsdis.close();
			System.out.println();
			  System.out.println(W_list.size());
			System.out.println();

		}
		Text map_key=new Text();
		Text map_value=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] var=value.toString().split(",");
			ArrayList<Double> X_list=new ArrayList<Double>();
			for(int i=0;i<var.length-1;i++)
			{
				X_list.add(Double.valueOf(var[i])/100);
			}
			X_list.add(1.0);
		   double Y=Double.valueOf(var[var.length-1])/100;
		   for(int i=0;i<W_list.size();i++)
		   {
			//   map_key.set(i+":"+W_list.get(i));
			   double p=(getH(W_list,X_list)-Y)*0.05*X_list.get(i);
			 //  map_value.set(""+p);
			 //  context.write(map_key, map_value);
			   W_list.set(i, W_list.get(i)-p);
		   }
		   
		}
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for(int i=0;i<W_list.size();i++)
			{
				System.out.println(W_list.get(i));
			}
			   for(int i=0;i<W_list.size();i++)
			   {
				   map_key.set(i+"");
				   map_value.set(""+W_list.get(i));
				   context.write(map_key, map_value);
			   }
		}
	}
	
	public static class DataReducer extends Reducer<Text,Text,Text,Text>
	{
        ArrayList<Double> W_list=new ArrayList<Double>();
        Text key=new Text();
        Text value=new Text();
        FSDataOutputStream fsdos;
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			//fsdos.close();
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration conf=context.getConfiguration() ;
			Path p=new Path(conf.get("HDFS"));
			 FileSystem fs = p.getFileSystem ( conf) ;
			// fsdos=fs.create(new Path(conf.get("model")));
		}

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
				throws IOException, InterruptedException {
			System.out.println("=============");
			// TODO Auto-generated method stub
			double sum=0;
			double count=0;
			for(Text val:arg1)
			{
				sum=sum+Double.valueOf(val.toString());
				count++;
			}
			System.out.println("sum="+sum);
			String[] var=arg0.toString().split(":");
			int index=Integer.valueOf(var[0]);
			double Wi=sum/count;
			System.out.println(Wi);
			//fsdos.writeUTF(Wi+"");
			key.set(""+index);
			value.set(Wi+"");
			arg2.write(key, value);
		}
	
	}
	 public static class PredictMapper extends Mapper<LongWritable,Text,Text,Text>
	 {
		 @Override
		 protected void setup(Context context) throws IOException, InterruptedException {
			 super.setup(context);
		 }

		 @Override
		 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 super.map(key, value, context);
		 }
	 }
     public static void main(String[] args) throws Exception
     {
    	   Constants.init();
    		//读取路径
    	   	Configuration conf=new Configuration();
    	 	conf.addResource(new Path(Constants.HADOOP_PATH+"/etc/hadoop/core-site.xml"));
    	   	String hdfs=conf.get("fs.default.name");
    	   	System.out.println(hdfs);
    	   	
    	   	Path p=new Path(hdfs);
    	   	conf.setStrings("HDFS", hdfs);
    	   	FileSystem fs=p.getFileSystem(conf);
    	   	String parentpath=p.toString();
    		FSDataOutputStream fsos=fs.create(new Path(parentpath+"/user/mengfanshan/LinearRegression/model.txt"));
    		for(int i=0;i<21;i++)
    		{
                fsos.writeUTF("0");
    		}
            fsos.close();
            conf.set("model",parentpath+"/user/mengfanshan/LinearRegression/model.txt" );
    	   String 	trainInputPath=parentpath+"/user/mengfanshan/LinearRegression/train/LinearRegression.data";
    	   	String predictInputPath=parentpath+"/home/LogisticRegression/predict/LogisticRegression.data";
    	   	String outputPath=parentpath+"/user/mengfanshan/LinearRegression/train/output";
    	   	int count=0;
    	   	while(count<1)
    	   	{
    	   		count++;
    	   		fs.delete(new Path(outputPath));
    	    	Job job1=new Job(conf);
    	       	job1.setJarByClass(LogisticRegission.class);
    	       	job1.setMapperClass(DataMapper.class);
    	       	job1.setReducerClass(DataReducer.class);
    	       	job1.setMapOutputKeyClass(Text.class);
    	       	job1.setMapOutputValueClass(Text.class);
    	       	job1.setOutputKeyClass(Text.class);
    	       	job1.setOutputValueClass(Text.class);
    	      // 	job1.setOutputFormatClass ( FileOutputFormat.class ) ;
    	       	FileInputFormat.addInputPath ( job1 , new Path ( trainInputPath ) ) ;
    	       	FileOutputFormat.setOutputPath( job1 , new Path ( outputPath ) ) ;
    	       	job1.waitForCompletion(true);  			 
    	   	}
     }
}
