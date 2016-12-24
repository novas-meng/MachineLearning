package com.novas;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Constants {
   public static String HADOOP_PATH;
	public static String ROOT_PATH;
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
