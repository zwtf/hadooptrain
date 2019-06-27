package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
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


public class JoinBean {
	
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(JoinBean.class);

		 job.setMapperClass(JoinBeanMapper.class);
		 job.setReducerClass(JoinBeanReducer.class);

		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(OrderBean.class);

		 job.setOutputKeyClass(OrderBean.class);
		 job.setOutputValueClass(NullWritable.class);

		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));

		 boolean res = job.waitForCompletion(true);
		 System.exit(res?0:1);
		 }
	
	static class JoinBeanMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
		OrderBean orderBean ;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, OrderBean>.Context context)
					throws IOException, InterruptedException {

			}
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,OrderBean>.Context context) 
						throws IOException ,InterruptedException {
			String line = value.toString();
			String[] s = line.split("\t");
			 FileSplit fileSplit= (FileSplit)context.getInputSplit();
			 String pid = "";
			 //根据名称判断文件类型,并创建自定义类
			String name = fileSplit.getPath().getName();
			if (name.substring(0, 5).equals("order")) {
				pid = s[1];
				orderBean.set(Integer.parseInt(s[0]),pid,Integer.parseInt(s[2]),"","0");
			}
			else {
				pid=s[0];
				orderBean.set(0,pid,0,s[1],"1");
			}
			 context.write(new Text(orderBean.getpId()), orderBean);
			
		};
	}
	
	 static class JoinBeanReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
		
		 @Override
		 protected void reduce(Text name, Iterable<OrderBean> beans,
				Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			 //根据flag判断键值对类型,把product文件放进一个数组
			 OrderBean pdBean = new OrderBean();
			 ArrayList<OrderBean> orderBeans = new ArrayList<OrderBean>();
			 try {
			 for (OrderBean bean : beans) {
			 // product
			if ("1".equals(bean.getFlag())) {
			 BeanUtils.copyProperties(pdBean, bean);
			 }else{
			 OrderBean odbean = new OrderBean();
			 BeanUtils.copyProperties(odbean, bean);
			 orderBeans.add(odbean);
			 }
			 }
			 } catch (Exception e) {
			 }
			 
			 for(OrderBean bean : orderBeans){
			 bean.setpId(pdBean.getpId());
			 context.write(bean, NullWritable.get());
			 }
			 }
			 
		}
	 }
	
	 class OrderBean implements Writable{

		int oId ;
		String pId;
		int volume ;
		String brand;
		//文件类标记 0即为order类 1即为product类
		String flag;
		
		public void write(DataOutput out) throws IOException {
			out.writeInt(oId);
			out.writeBytes(pId); 
			out.writeInt(volume);
			out.writeBytes(pId);
			out.writeUTF(flag);
		}

		public Object getFlag() {
			// TODO Auto-generated method stub
			return null;
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
		
		@Override
			public String toString() {
				return oId + " " + pId + " " + volume + " " + brand + " ";
			}
		
	}
	

