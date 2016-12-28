package com.novas;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Constants {
	//hadoop路径
   public static String HADOOP_PATH;
	//项目的根目录
	public static String ROOT_PATH;
	//属性服务的端口号
	public static int port;
	//属性服务的ip
	public static String ip;
	public static String _1_2_UNDONE="1_2:UNDONE";
	public static String _2_4_UNDONE="2_4:UNDONE";
	public static String _3_4_UNDONE="3_4:UNDONE";
	public static String _4_5_UNDONE="4_5:UNDONE";

	public static String _1_2_RUNNING="1_2:RUNNING";
	public static String _2_4_RUNNING="2_4:RUNNING";
	public static String _3_4_RUNNING="3_4:RUNNING";
	public static String _4_5_RUNNING="4_5:RUNNING";

	public static String _1_2_DONE="1_2:DONE";
	public static String _2_4_DONE="2_4:DONE";
	public static String _3_4_DONE="3_4:DONE";
	public static String _4_5_DONE="4_5:DONE";
   public static void init()
   {
	   File f=new File("/etc/profile");
	   try {
		BufferedReader br=new BufferedReader(new FileReader(f));
		String line=br.readLine();
		while(line!=null)
		{

			if(line.contains("HADOOP_HOME"))
			{
				HADOOP_PATH=line.split("=")[1];
				break;
			}
			line=br.readLine();
		}
		   br.close();
		   br=new BufferedReader(new FileReader(f));
		   line=br.readLine();
		   while (line!=null)
		   {
			   if(line.contains("NOVAS_HOME"))
			   {
				   String[] var=line.split("=");

				   Constants.ROOT_PATH=var[1];
				   if(Constants.ROOT_PATH.endsWith("/"))
				   {
					   Constants.ROOT_PATH=Constants.ROOT_PATH.substring(0,Constants.ROOT_PATH.length()-1);
				   }
				   System.out.println(Constants.ROOT_PATH);
				   break;
			   }
			   line=br.readLine();
		   }
		   br.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	   
   }
}
