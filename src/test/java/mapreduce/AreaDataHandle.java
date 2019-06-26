package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 一大堆面数据定点转换成面ID和点的对应关系:
 * 		(没有Reducer)
 * 		缺点:匹配出来的点由于是遍历取的,顺序被打乱了,可以怎么修改呢:
 * 
 */
public class AreaDataHandle {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);		
		job.setJarByClass(AreaDataHandle.class);
		
		job.setMapperClass(AreadataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
	
	static class  AreadataMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected  void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
					String line = value.toString();
					String areaId = line.split("\t")[0];
					String areaData = line.split("\t")[1];
					
					for (String pointData : areaData.split(";")) {
						context.write(new Text(areaId), new Text(pointData));
					}
		}
	}
}
