package com.novas;

import com.sun.xml.bind.v2.runtime.reflect.opt.Const;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Controller {
	private static ParamsManager manager;

	//参数形式 端口号 用户名 时间戳 算法名称 算法参数
     public static void main(String[] args)
     {
    	 Constants.init();
    	 System.out.println(Constants.HADOOP_PATH);
    	 manager=ParamsManager.getParamsManagerInstance();
    	 if(args.length!=0)
    	 {
			 Constants.ip=args[0];
			 Constants.port=Integer.parseInt(args[1]);
			 String username=args[2];
    		 long time=Long.parseLong(args[3]);
    		 String name=args[4];
    		 String params=args[5];
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
