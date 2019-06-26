package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	/**
	 * mapreduce方法实现统计多个文件中最高气温
	 * 	map:清洗数据,取出文件内的和全部温度信息数据
	 *		key:年份字符串	value:最高气温Intwritable
	 * 
	 * reduce: 每个key中取温度最高的Intwritable
	 * @author wx
	 *
	 */
public class MaxTemperature {
	public static void main(String[] args) throws Exception {
		System.out.println(args.length);
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input Path> <OutputPath>");			
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		//这个输出路径在运行前是不应该存在的（防止数据被覆盖）
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//不必指定JAR文件名称，在这里传入一个类，hadoop可以直接根据这个类查找包含的Jar文件
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTempratureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MaxTempratureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

//版本问题导致的没有Mapper这一接口吗?，以下写法是错误的
//public class MaxTemperatureMapper 
//extends MapReduceBase implements  Mapper<LongWritable,Text,Text,IntWritable> {
//答:	因为在main函数中是按照类名来调用方法的,所以要将Reduce内部类申明为静态的。
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final int MISSING = 9999;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
				 airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			String quality = line.substring(92, 93);
			if (airTemperature != MISSING && quality.matches("[01459]"));
			{
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}

}
