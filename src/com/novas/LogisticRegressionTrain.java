package com.novas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
用户选择的维度信息，最终会上传hdfs上，具体文件夹为timestamp/columnchoosed.txt
文件格式为
0,1,2,4,5 依次类推（从0开始）
 */
public class LogisticRegressionTrain implements Algo
{
    public static String readInputStream(FSDataInputStream fsDataInputStream)throws IOException
    {
        byte[] bytes=new byte[1024*1024];
        byte[] tmp=new byte[0];
        int length=0;
        while ((length=fsDataInputStream.read(bytes))!=-1)
        {
            byte[] var=new byte[tmp.length];
            System.arraycopy(tmp,0,var,0,tmp.length);
            System.out.println("length="+length);
            tmp=new byte[tmp.length+length];
            System.arraycopy(var,0,tmp,0,var.length);
            System.arraycopy(bytes,0,tmp,var.length,length);
        }
        return new String(tmp);
    }

    public static class DataMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        double alpha;
        double l;
        String regular;
        String columnchoosed;
        int columncount;
        HashMap<Integer,Integer> columnChoosedMap=new HashMap<>();
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
        ArrayList<Double> W_list=new ArrayList<Double>(100000);
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            Configuration conf=context.getConfiguration() ;
            alpha=conf.getDouble("alpha",0.05);
            columncount=conf.getInt("columncount",0);
            columnchoosed=conf.get("columnchoosed");
            l=conf.getDouble("l",50);
            regular=conf.get("regular","None");
            System.out.println("alpha="+alpha+"  "+l+"  "+regular);
            Path p=new Path(conf.get("HDFS"));
            //读取模型的初始权值
            FileSystem fs = p.getFileSystem ( conf) ;
            FSDataInputStream fsdis=fs.open(new Path(conf.get("model")));
            for(int i=0;i<=columncount;i++)
            {
                W_list.add(Double.valueOf(fsdis.readUTF()));
            }
            fsdis.close();
            //读取用户选择了哪些列
            FSDataInputStream fsdis_column_choosed=fs.open(new Path(columnchoosed));
            String columns=readInputStream(fsdis_column_choosed).trim();
            fsdis_column_choosed.close();
            System.out.println("colums="+columns);
            String[] var=columns.split(",");
            for (int i=0;i<var.length;i++)
            {
                System.out.println(Integer.valueOf(var[i]));
                columnChoosedMap.put(Integer.parseInt(var[i]),1);
            }
            System.out.println(columnChoosedMap);
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
            //获取相关系数

            String[] var=value.toString().split(",");
            ArrayList<Double> X_list=new ArrayList<Double>();
            for(int i=0;i<var.length-1;i++)
            {
                if(columnChoosedMap.containsKey(i))
                {
                    X_list.add(Double.valueOf(var[i])/100);
                }
            }
            X_list.add(1.0);
            double Y=Double.valueOf(var[var.length-1])/100;
            for(int i=0;i<W_list.size();i++)
            {
                double var1=getH(W_list,X_list)-Y;
                double var2=X_list.get(i);
                double var3=0;
                if(regular.equals("None"))
                {
                    var3=W_list.get(i)-var1*var2*alpha;
                }
                else if(regular.equals("L1"))
                {
                     var3=W_list.get(i)-var1*var2*alpha-alpha*l;
                }
                else if(regular.equals("L2"))
                {
                     var3=(1-alpha*l)*W_list.get(i)-var1*var2*alpha;
                }
                W_list.set(i, var3);
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
    public  void run(long timestamp) throws IOException,ClassNotFoundException, InterruptedException
    {
        Constants.init();
        ParamsManager manager=ParamsManager.getParamsManagerInstance();

        //读取路径
        Configuration conf=new Configuration();
        conf.addResource(new Path(Constants.HADOOP_PATH+"/etc/hadoop/core-site.xml"));
        String hdfs=conf.get("fs.default.name");
        System.out.println(hdfs);

        Path p=new Path(hdfs);
        conf.setStrings("HDFS", hdfs);
        FileSystem fs=p.getFileSystem(conf);
        String parentpath=p.toString();
        FSDataOutputStream fsos=fs.create(new Path(parentpath+manager.getTmpDir(timestamp)+"model.txt"));
        int columncount=(Integer)manager.getParamsValue(timestamp,"columncount");
        for(int i=0;i<=columncount;i++)
        {
            fsos.writeUTF("0");
        }
        fsos.close();
        conf.set("model",parentpath+manager.getTmpDir(timestamp)+"model.txt" );
        String 	trainInputPath=parentpath+manager.getParamsValue(timestamp,"inputPath").toString();
        String outputPath=parentpath+"/"+timestamp+manager.getParamsValue(timestamp,"outputPath").toString();
        fs.delete(new Path(outputPath));
        System.out.println(trainInputPath);
        System.out.println(outputPath);
        int loopcount=(Integer)manager.getParamsValue(timestamp,"loopcount");
        int count=0;
        double alpha=(Double)manager.getParamsValue(timestamp,"alpha");
        double l=(Double)manager.getParamsValue(timestamp,"l");
        String regular=manager.getParamsValue(timestamp,"regular").toString();
        conf.setDouble("alpha",alpha);
        conf.setDouble("l",l);
        conf.set("regular",regular) ;
        //columnchoosed表示用户选择了哪些列，columncount表示用户选择的列个数
        conf.set("columnchoosed",parentpath+manager.getTmpDir(timestamp)+"columnchoosed.txt");
        conf.setInt("columncount",columncount);
        while(count<loopcount)
        {
            count++;
            fs.delete(new Path(outputPath));
            Job job1=new Job(conf);
            job1.setJarByClass(LogisticRegressionTrain.class);
            job1.setMapperClass(DataMapper.class);
            job1.setReducerClass(DataReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath ( job1 , new Path ( trainInputPath ) ) ;
            FileOutputFormat.setOutputPath( job1 , new Path ( outputPath ) ) ;
            job1.waitForCompletion(true);
        }
    }
}
