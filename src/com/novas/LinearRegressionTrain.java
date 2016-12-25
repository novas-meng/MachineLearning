package com.novas;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.math3.analysis.function.Constant;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.util.IO;

/*
用户选择的维度信息，最终会上传hdfs上，具体文件夹为timestamp/columnchoosed.txt
文件格式为
0,1,2,4,5 依次类推（从0开始）
 */
public class LinearRegressionTrain implements Algo
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
    public static double getH(ArrayList<Double> W_list,ArrayList<Double> X_list)
    {
        double sum=0;
        for(int i=0;i<W_list.size();i++)
        {
            double m=W_list.get(i);
            double n=X_list.get(i);
            sum=sum+m*n;
        }
        return sum;
    }
    public static class DataMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        double alpha;
        double l;
        String regular;
        String columnchoosed;
        int columncount;
        int label;
        HashMap<Integer,Integer> columnChoosedMap=new HashMap<>();

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
            label=conf.getInt("label",0);
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
            double Y=Double.valueOf(var[label])/100;
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
    //预测Mapper
    public static class PredictMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
    {
        int columncount;
        String columnchoosed;
        HashMap<Integer,Integer> columnChoosedMap=new HashMap<>();
        ArrayList<Double> W_list=new ArrayList<Double>(100000);

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            Configuration conf=context.getConfiguration() ;
            columncount=conf.getInt("columncount",0);
            columnchoosed=conf.get("columnchoosed");
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
            for (int i=0;i<var.length;i++) {
                System.out.println(Integer.valueOf(var[i]));
                columnChoosedMap.put(Integer.parseInt(var[i]), 1);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // super.map(key, value, context);
            System.out.println(value.toString());
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
            System.out.println(X_list.get(0)+"  "+W_list.get(0));
            double predict_result=getH(W_list,X_list);
            System.out.println(predict_result);
            context.write(value,new DoubleWritable(predict_result));
        }
    }
//预测Reducer
    public static class PredictrReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
    {
        @Override
        protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,Context arg2)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for(DoubleWritable val:arg1)
            {
                arg2.write(arg0, val);

            }
        }
    }

    public  void run(String username,long timestamp) throws IOException, ClassNotFoundException, InterruptedException {
        Constants.init();
//获取参数管理器
        ParamsManager manager=ParamsManager.getParamsManagerInstance();
       // ConnectionManager connectionManager=
              //  ConnectionManager.getConnectionManagerInstance("127.0.1.1",9084);
        //读取路径
        System.out.println("HADOOP_PATH="+Constants.HADOOP_PATH);
        Configuration conf=new Configuration();
        conf.addResource(new Path(Constants.HADOOP_PATH+"/etc/hadoop/core-site.xml"));
        String hdfs=conf.get("fs.default.name");
        System.out.println(hdfs);

        Path p=new Path(hdfs);
        conf.setStrings("HDFS", hdfs);
        // connectionManager.sendConf("{4,SUCCESS,"+timestamp+"}");
       // connectionManager.sendConf("{4,SUCCESS,"+timestamp+",Reading the configuration is finished.}");

        FileSystem fs=p.getFileSystem(conf);
        //parentpath为hdfs路径+用户名
        String parentpath=p.toString();
        parentpath=parentpath+"/"+username;
        String modelPath=parentpath+"/"+manager.getTmpDir(timestamp)+"model.txt";
        //初始化model.txt文件
        FSDataOutputStream fsos=fs.create(new Path(modelPath));
        int columncount=(Integer)manager.getParamsValue(timestamp,"columncount");
       for(int i=0;i<=columncount;i++)
        {
            fsos.writeUTF("0");
       }
        fsos.close();
        //获取用户选择的标签列
        int  label=(Integer)manager.getParamsValue(timestamp,"label");
        conf.setInt("label",label);
        //将用户训练输入文件从本地上传到hdfs
        System.out.println("上传训练文件到hdfs中....");
        String trainInputFileName=manager.getParamsValue(timestamp,"trainInputPath").toString();
        String localInputPath=Constants.ROOT_PATH+"/users/"+username+"/"+trainInputFileName;
        String 	hdfsInputPath=parentpath+"/"+trainInputFileName;
        fs.copyFromLocalFile(false,new Path(localInputPath),
                new Path(hdfsInputPath));
        //用户将预测输入文件上传到hdfs中
        System.out.println("上传预测文件到hdfs中....");

        String predictInputFileName=manager.getParamsValue(timestamp,"predictInputPath").toString();
        String localPredictInputPath=Constants.ROOT_PATH+"/users/"+username+"/"+predictInputFileName;
        System.out.println(localPredictInputPath);
        String 	hdfsPredictInputPath=parentpath+"/"+predictInputFileName;
        fs.copyFromLocalFile(false,new Path(localPredictInputPath),
                new Path(hdfsPredictInputPath));


        conf.set("model",parentpath+"/"+manager.getTmpDir(timestamp)+"model.txt" );
        //设置hdfs输出路径和本地输出路径，程序结束时，将文件下载到本地
        String localOutputPath= Constants.ROOT_PATH+"/users/"+username+"/"+timestamp+"/modelOutput/"
                +manager.getParamsValue(timestamp,"modelOutputPath").toString();
        String hdfsOutputPath=parentpath+"/"+timestamp+"/modelOutput";
        //设置hdfs预测输出路径和本地预测输出路径 ，程序结束时，下载到本地
        String localPredictOutputPath=Constants.ROOT_PATH+"/users/"+username+"/"+timestamp+"/predictOutput/"
                +manager.getParamsValue(timestamp,"predictOutputPath").toString();
        String hdfsPredictOutputPath=parentpath+"/"+timestamp+"/predictOutput";


        System.out.println(hdfsInputPath);
        System.out.println(hdfsOutputPath);
        //配置参数
        int loopcount=(Integer)manager.getParamsValue(timestamp,"loopcount");
        int count=0;
        double alpha=(Double)manager.getParamsValue(timestamp,"alpha");
        double l=(Integer)manager.getParamsValue(timestamp,"l");
        String regular=manager.getParamsValue(timestamp,"regular").toString();
        conf.setDouble("alpha",alpha);
        conf.setDouble("l",l);
        conf.set("regular",regular) ;
        //将用户选择的属性列上传
        fs.copyFromLocalFile(false,new Path(Constants.ROOT_PATH+"/users/"+username+"/"
                +timestamp+"/"+"columnchoosed.txt")
                ,new Path(parentpath+"/"+manager.getTmpDir(timestamp)+"columnchoosed.txt"));
        //columnchoosed表示用户选择了哪些列，columncount表示用户选择的列个数
        conf.set("columnchoosed",parentpath+"/"+manager.getTmpDir(timestamp)+"columnchoosed.txt");
        conf.setInt("columncount",columncount);
        //   connectionManager.sendConf("the task conf is over");
       // connectionManager.sendConf("{4,SUCCESS,"+timestamp+",Setting the Params is finished.}");

        while(count<loopcount)
        {
          //  connectionManager.sendConf("{4,SUCCESS,"+timestamp+",the "+count+"th MapperReducer is running.}");
            fs.delete(new Path(hdfsOutputPath));
            Job job1=new Job(conf);
            job1.setJarByClass(LinearRegressionTrain.class);
            job1.setMapperClass(DataMapper.class);
            job1.setReducerClass(DataReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath ( job1 , new Path ( hdfsInputPath ) ) ;
            FileOutputFormat.setOutputPath( job1 , new Path ( hdfsOutputPath ) ) ;
            job1.waitForCompletion(true);
          //  connectionManager.sendConf("{4,SUCCESS,"+timestamp+",the "+count+"th MapperReducer is finished.}");
            count++;

        }

        fs.copyToLocalFile(new Path(hdfsOutputPath+"/part-r-00000"),new Path(localOutputPath));
        //重新生成本地model.txt
        recreateMode(localOutputPath,columncount,modelPath,fs);
        fs.delete(new Path(hdfsPredictOutputPath));
        Job predictJob=new Job(conf);
        predictJob.setJarByClass(LinearRegressionTrain.class);
        predictJob.setMapperClass(PredictMapper.class);
        predictJob.setReducerClass(PredictrReducer.class);
        predictJob.setMapOutputKeyClass(Text.class);
        predictJob.setMapOutputValueClass(DoubleWritable.class);
        predictJob.setOutputKeyClass(Text.class);
        predictJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath ( predictJob , new Path ( hdfsPredictInputPath ) ) ;
        FileOutputFormat.setOutputPath( predictJob , new Path ( hdfsPredictOutputPath ) ) ;
        predictJob.waitForCompletion(true);
        fs.copyToLocalFile(new Path(hdfsPredictOutputPath+"/part-r-00000"),
                new Path(localPredictOutputPath));
        // connectionManager.sendConf("the task is over");
    }
    //重新生成model，用训练好的model生成
    public  void recreateMode(String localPath,int columncount,String modelPath,FileSystem fs)throws IOException
    {
        BufferedReader bufferedReader=new BufferedReader(new FileReader(localPath));
        String line=bufferedReader.readLine();
        double[] w=new double[columncount+1];
        while (line!=null)
        {
           String[] var=line.split("\t");
            w[Integer.valueOf(var[0])]=Double.valueOf(var[1]);
            line=bufferedReader.readLine();
        }
        bufferedReader.close();
          FSDataOutputStream fsos=fs.create(new Path(modelPath));
          for(int i=0;i<=columncount;i++)
          {
               fsos.writeUTF(w[i]+"");
           }
          fsos.close();
    }

}
