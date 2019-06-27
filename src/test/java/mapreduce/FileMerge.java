package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * mapreduce实现文件合并
 * 	思路:
 * 		map:key是NullWritable,value是文件的字节数组,类型是BytesWritable
 * 		reduce:注意OutputValue类型要用SequenceFileOutPutFormat,否则默认是	TextOutput输出的是对象ID
 * @author wx
 *
 */

public class FileMerge {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf);
		job.setJarByClass(FileMerge.class);
		
		job.setMapperClass(FileMergeMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(BytesWritable.class);
		
		//设置输入类型为自定义类型,输出类型为SequenceFileOutputFormat型
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(BytesWritable.class);
		
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.111.109:9400/littlefiles"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.111.109:9400/merge"));
		 
		
		job.waitForCompletion(true);
	}
	
	
	//自定义文件类型
	static class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable>{
	
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
//			// 默认调用父类的方法,父类直接返回true,也就是默认可以分片的
//			return super.isSplitable(context, filename);
			//设置文件不可分片,保证每个文件形成一个键值对
			return false;
		}
		
		@Override
		public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			MyRecordReader recordReader = new MyRecordReader(); 
			recordReader.initialize(split, context);
			  return recordReader;
		}		
	}
	
	//重写recordreader
	//该类主要负责返回MapInputKey和MapInputValue,重写该类后告诉程序怎么实现一次读一份文件以及返回的类型)
	//	关键方法: 
				//	nextKeyValue():获取下一个key 和value
				//	getCurrentKey() || getCurrentValue(): 获取并返回Key和Value
	static class MyRecordReader extends RecordReader<NullWritable, BytesWritable>{
		
			 private FileSplit fileSplit;
			 private Configuration conf;
			 private BytesWritable value = new BytesWritable();
			 private boolean processed = false;
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			//强转,向下转型,这样写更安全,不过该例输入都是文件类split,加不加都一样
			if(split instanceof FileSplit) {
				this.fileSplit = (FileSplit)split;
				conf = context.getConfiguration();
			}
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(!processed) {
				//分好的fileSplit实例,转成HDFS文件系统实例,再转成IO流,读到字节数组里面
				//因为是一整个文件,所以可以根据分片所在路径实例化成文件系统实例
				//文件拆分呢?
				byte[] fileBytes =new byte[(int) fileSplit.getLength()];
				Path path = fileSplit.getPath();
//				FileSystem fs = FileSystem.newInstance(conf);
				FileSystem fs = path.getFileSystem(conf);
				FSDataInputStream in =null;
				try {					
					in = fs.open(path);
					IOUtils.readFully(in, fileBytes, 0, fileBytes.length);
					value.set(fileBytes, 0, fileBytes.length);
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
			//读完才退出,取下一个文件
			return false;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
	//为什么不直接把filename作为key返回出来?
	public static class FileMergeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{
		private Text filenameKey;
		
		@Override
		protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
					InputSplit split = context.getInputSplit();
					Path path = ((FileSplit) split).getPath();
					filenameKey = new Text(path.toString());
		}
		@Override
		protected void map(NullWritable key, BytesWritable value,
				Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
					context.write(filenameKey, value);
		}
		
	}
	
	
}
