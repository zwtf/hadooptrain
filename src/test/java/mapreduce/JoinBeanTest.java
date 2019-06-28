package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;



/**
 * MapReduce实现join操作,将订单文件和信息文件join并输出
 * 		思路:在map里面根据文件名区分文件是订单还是信息文件,并打上标记
 * 			把PID作为key,自定义的bean对象作为value,实现
 * @author wx
 *
 */

/* 
 * 样例数据:
order
1001 P0001 200
1002 P0001 300
1003 P0002 400
1004 P0003 500
1004 P0004 20

product
P0001 XIAOMI
P0002 IPHONE
P0003 NOKIA
P0005 HuaWEi

最终结果:
1001 P0001 200 XIAOMI
1002 P0001 300 XIAOMI
1003 P0002 400 IPHONE
1004 P0003 500 NOKIA
1004 P0004 20

 */


public class JoinBeanTest {
	
	
	
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 
		 	//判断输入文件夹是否存在,,不存在就退出程序
			//判断输出文件夹是否存在,存在即删除	
		 	 Path outPathput = new Path(args[1]);
			 FileSystem FSOutput = FileSystem.get(URI.create(args[1]), conf);
			 System.out.println( FSOutput.exists(outPathput) ? "---true---" : "---false---");
			 if (FSOutput.exists(outPathput)) {
				System.out.println("Path Exists! deleted!");
				 System.out.println( FSOutput.exists(outPathput) ? "---true---" : "---false---");
				FSOutput.delete(outPathput, true);
			}else {
				System.out.println( FSOutput.exists(outPathput) ? "---true---" : "---false---");	
			}
		 
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(JoinBeanTest.class);

		 job.setMapperClass(JoinBeanTestMapper.class);
//		 job.setReducerClass(JoinBeanTestReducer.class);

		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(OrderBeanTest.class);

		 job.setOutputKeyClass(OrderBeanTest.class);
		 job.setOutputValueClass(NullWritable.class);

		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
		 boolean res = job.waitForCompletion(true);
		 System.exit(res?0:1);
		 }
	
	static class JoinBeanTestMapper extends Mapper<LongWritable, Text, Text, OrderBeanTest>{
		OrderBeanTest OrderBeanTest  = new OrderBeanTest();  //  ----
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, OrderBeanTest>.Context context)
					throws IOException, InterruptedException {

			}
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,OrderBeanTest>.Context context) 
						throws IOException ,InterruptedException {
			 String line = value.toString();
			 String[] s = line.split("\t");
			 int oId = 0; 						// ----
			 String pid = "Unknown";
			 int volume = 0;
			 String brand = "Unknown";
			 FileSplit fileSplit= (FileSplit)context.getInputSplit();
			 //根据名称判断文件类型,并创建自定义类
			String name = fileSplit.getPath().getName();
			if (name.substring(0, 5).equals("order")) {
				oId = Integer.parseInt(s[0]);
				pid = s[1];
				volume = Integer.parseInt(s[2]);
				OrderBeanTest.set(oId,pid,volume,brand,"0");
			}
			else {				
				pid=s[0];
				brand = s[1];
				OrderBeanTest.set(0,pid,0,brand,"1");
			}
			 context.write(new Text(pid), OrderBeanTest);	
		}
	}
	
	static class JoinBeanTestReducer extends Reducer<Text, OrderBeanTest, OrderBeanTest, NullWritable>{
		@Override
		protected void reduce(Text arg0, Iterable<OrderBeanTest> arg1,
				Reducer<Text, OrderBeanTest, OrderBeanTest, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (OrderBeanTest obt : arg1) {
				context.write(obt,NullWritable.get());
			}
		}
	}
	
//	 static class JoinBeanTestReducer extends Reducer<Text, OrderBeanTest, OrderBeanTest, NullWritable>{
//		
//		 @Override
//		 protected void reduce(Text name, Iterable<OrderBeanTest> beans,
//				Reducer<Text, OrderBeanTest, OrderBeanTest, NullWritable>.Context context)
//				throws IOException, InterruptedException {
//			 //根据flag判断键值对类型,把product文件放进一个数组
//			 String brand = "UNKNOWN";
//			 ArrayList<OrderBeanTest> OrderBeanTests = new ArrayList<OrderBeanTest>();
//			 try {
//			 for (OrderBeanTest bean : beans) {
//			 // product
//			if ("1".equals(bean.getFlag())) {
//				 brand = bean.getBrand();
//
//			 }
//			context.write(bean, NullWritable.get());
////			else{
//////				 OrderBeanTest odbean = new OrderBeanTest();
//////				 BeanUtils.copyProperties(odbean, bean);
////				 OrderBeanTests.add(bean);
////			 }
//			 }
////				for (OrderBeanTest  bean1: OrderBeanTests) {
////					
////					 context.write(bean1, NullWritable.get());
////				}
////			 for (OrderBeanTest bean1 : beans) {
//////				if ("0".equals(bean1.getFlag())) {
////					 bean1.setBrand(brand);
////					 context.write(bean1, NullWritable.get());
//////				 }
////				}
//			 } catch (Exception e) {
//			 }
//
////			 for(OrderBeanTest bean : OrderBeanTests){
//////				 if (bean.getpId() == "") {	
//////					 bean.set(100, "pid", 200, "is", "null!");
//////				}
////			 bean.setBrand(brand);
////			 context.write(bean, NullWritable.get());
////			 }
//			 }
//			 
//		}
//
//
//		
}
	
	 class OrderBeanTest implements Writable{

		int oId ;
		String pId;
		int volume ;
		String brand;
		//文件类标记 0即为order类 1即为product类
		String flag;
				
		public void write(DataOutput out) throws IOException {
			out.writeInt(oId);
			out.writeUTF(pId);   //   ---  
			out.writeInt(volume);
			out.writeUTF(brand);
			out.writeUTF(flag);
		}



		public void readFields(DataInput in) throws IOException {
			this.oId = in.readInt();
			this.pId = in.readUTF();
			this.volume = in.readInt();
			this.brand = in.readUTF();
			this.flag = in.readUTF();
		}

		public void set(int oId ,String pId,int volume,String brand,String flag) {
			this.oId = oId;
			this.pId = pId;
			this.volume = volume;
			this.brand = brand;
			this.flag = flag;
		}
		
		public int getoId() {
			return oId;
		}
		
		public void setoId(int oId) {
			this.oId = oId;
		}
		
		public String getpId() {
			return pId;
		}
		
		public void setpId(String pId) {
			this.pId = pId;
		}
		
		public int getVolume() {
			return volume;
		}
		
		public void setVolume(int volume) {
			this.volume = volume;
		}
		
		public String getBrand() {
			return brand;
		}
		
		public void setBrand(String brand) {
			this.brand = brand;
		} 
		
		public String getFlag() {
			return flag;
		}
		
		@Override
			public String toString() {
				return oId + " " + pId + " " + volume + " " + brand + " " + flag;
			}
}
