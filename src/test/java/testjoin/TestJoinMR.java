package testjoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TestJoinMR {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJarByClass(TestJoinMR.class);			
			job.setMapperClass(JoinMapper.class);
			job.setReducerClass(JoinReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TestBean.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TestBean.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));		
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			boolean res = job.waitForCompletion(true);
			System.exit(res?0:1);
	}

	 static class JoinMapper extends Mapper<LongWritable, Text,Text, TestBean> {
		 
		 TestBean testBean;
		 @Override
		protected void setup(Mapper<LongWritable, Text,Text, TestBean>.Context context)
				throws IOException, InterruptedException {
			 testBean = new TestBean();	

		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, TestBean>.Context context)
				throws IOException, InterruptedException {
			 		String pid;
					//获取文件内容
					String line = value.toString();
					String[] strings = line.split("\t");
					//获取文件名称
					FileSplit fileSplit = (FileSplit) context.getInputSplit();
					String filename = fileSplit.getPath().getName();
					if (filename.startsWith("order") && strings.length > 2) {
						pid = strings[1];
						testBean.set(Integer.parseInt(strings[0]),pid, Integer.parseInt(strings[2]), "yet not match", "0");						
					}else {
						pid = strings[0];
						testBean.set(0000, pid, 0, strings[1], "1");
					}
					context.write(new Text(pid), testBean);
		}	
	}
	 
	 //运行结果:同样的程序,将原Order文件从一份替换成两份会导致结果错误且不正确,目前原因未知
	 //目前预测是循环逻辑判断错误,但是把源代码原封不动的拷贝修改后得到的结果仍然不正确
	 	//源代码执行结果完全正确且符合要求
	 static class JoinReducer extends Reducer<Text, TestBean, TestBean, NullWritable>{
		 TestBean pdBean = new TestBean();
		private ArrayList<TestBean> orderBeans =new  ArrayList<TestBean>();

		@Override
		protected void reduce(Text pid, Iterable<TestBean> beans,
				Reducer<Text, TestBean, TestBean, NullWritable>.Context context) throws IOException, InterruptedException {
						try {
							for (TestBean testBean : beans) {
								if (testBean.getFlag().equals("1")) {
										BeanUtils.copyProperties(pdBean, testBean);
								}else {
									 TestBean odbean = new TestBean();
									 BeanUtils.copyProperties(odbean, testBean);
									 orderBeans.add(odbean);
								}
							}
						}
						catch (Exception e) {
						}
						  for(TestBean bean : orderBeans){
							  bean.setBrand(pdBean.getBrand());
							  context.write(bean, NullWritable.get());
							  }
					
		}
	}
	 

//	 该reduce输入输出结果:
//		 输入:
//		 P0001	oid:0pid:P0001volume:0brand:XIAOMIflag:1
//		 P0001	oid:2002pid:P0001volume:600brand:yet not matchflag:0
//		 P0001	oid:2001pid:P0001volume:500brand:yet not matchflag:0
//		 P0001	oid:1002pid:P0001volume:300brand:yet not matchflag:0
//		 P0001	oid:1001pid:P0001volume:200brand:yet not matchflag:0
//		 P0002	oid:1003pid:P0002volume:400brand:yet not matchflag:0
//		 P0002	oid:0pid:P0002volume:0brand:IPHONEflag:1
//		 P0002	oid:2003pid:P0002volume:700brand:yet not matchflag:0
//		 P0003	oid:2004pid:P0003volume:800brand:yet not matchflag:0
//		 P0003	oid:1004pid:P0003volume:500brand:yet not matchflag:0
//		 P0003	oid:0pid:P0003volume:0brand:NOKIAflag:1
//		 P0004	oid:2004pid:P0004volume:30brand:yet not matchflag:0
//		 P0004	oid:1004pid:P0004volume:20brand:yet not matchflag:0
//		 P0005	oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		 输出:
//		  id:2001pid:P0001volume:500brand:XIAOMIflag:0
//		  oid:2001pid:P0001volume:500brand:XIAOMIflag:0
//		  oid:2001pid:P0001volume:500brand:XIAOMIflag:0
//		  oid:2001pid:P0001volume:500brand:XIAOMIflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:1003pid:P0002volume:400brand:IPHONEflag:0
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:0pid:P0003volume:0brand:NOKIAflag:1
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:2004pid:P0004volume:30brand:NOKIAflag:0
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1
//		  oid:0pid:P0005volume:0brand:HuaWEiflag:1

//	 static class JoinReducer extends Reducer<Text, TestBean, TestBean, NullWritable>{
//		private String brand;
//		private ArrayList<TestBean> orderArrayList =new  ArrayList<TestBean>();
//
//		@Override
//		protected void reduce(Text pid, Iterable<TestBean> beans,
//				Reducer<Text, TestBean, TestBean, NullWritable>.Context context) throws IOException, InterruptedException {
//					for (TestBean testBean : beans) {
//						if (testBean.getFlag().equals("1")) {
//							brand = testBean.getBrand();
//						}else {
//							orderArrayList.add(testBean);
//						}
//					}
//					for (TestBean ordBean : orderArrayList) {
//						ordBean.setBrand(brand);
//						context.write(ordBean, NullWritable.get());
//					}
//		}
//	}
	
}
