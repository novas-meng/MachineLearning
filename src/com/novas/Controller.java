package com.novas;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Controller {
	private static ParamsManager manager;

	//参数形式 用户名 时间戳 算法名称 算法参数
     public static void main(String[] args)
     {
    	 Constants.init();
    	 System.out.println(Constants.HADOOP_PATH);
    	 manager=ParamsManager.getParamsManagerInstance();
    	 if(args.length!=0)
    	 {
			 String username=args[0];
    		 long time=Long.parseLong(args[1]);
    		 String name=args[2];
    		 String params=args[3];
    		 try {
				Object obj=ClsUtils.generate(name, params);
				manager.addParamsValue(time, obj);
				Class<?> cls=Class.forName("com.novas."+name);
				Algo algo=(Algo) cls.newInstance();
				try {
					algo.run(username,time);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (ClassNotFoundException | InstantiationException
					| IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		 
    	 }
     }
}
