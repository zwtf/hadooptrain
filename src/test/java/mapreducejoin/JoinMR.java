package mapreducejoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class JoinMR {
  static class JoinMRMapper extends Mapper<LongWritable, Text
, Text, InfoBean> {
  InfoBean bean = new InfoBean();
  Text k = new Text();
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  String line = value.toString(); 
  String[] fields = line.split("\t");
  FileSplit inputSplit = (FileSplit) context.getInputSplit();
  String filename = inputSplit.getPath().getName();
  String pid = "";
  if (filename.startsWith("order")) {
  pid = fields[2];
  bean.set(Integer.parseInt(fields[0]), fields[1]
, pid, Integer.parseInt(fields[3]), "", 0, 0, "0");
  } else {
  pid = fields[0];
  bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
  }
  k.set(pid);
  context.write(k, bean);
  }
  }
  static class JoinMRReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
  @Override
  protected void reduce(Text pid, Iterable<InfoBean> beans, Context context)
  throws IOException, InterruptedException {
  InfoBean pdBean = new InfoBean();
  ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
  try {
  for (InfoBean bean : beans) {
  // product
if ("1".equals(bean.getFlag())) {
  BeanUtils.copyProperties(pdBean, bean);
  }else{
  InfoBean odbean = new InfoBean();
  BeanUtils.copyProperties(odbean, bean);
  orderBeans.add(odbean);
//  orderBeans.add(bean);
  }
  }
  } catch (Exception e) {
  }
  for(InfoBean bean : orderBeans){
	  bean.setPname(pdBean.getPname());
	  bean.setCategory_id(pdBean.getCategory_id());
	  bean.setPrice(pdBean.getPrice());
	  context.write(bean, NullWritable.get());
	  }
	  }
	  }
	  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf);

	  job.setJarByClass(JoinMR.class);

	  job.setMapperClass(JoinMRMapper.class);
	  job.setReducerClass(JoinMRReducer.class);

	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(InfoBean.class);
	  

	  job.setOutputKeyClass(InfoBean.class);
	  job.setOutputValueClass(NullWritable.class);
	  

	  FileInputFormat.setInputPaths(job, new Path(args[0]));

	  FileOutputFormat.setOutputPath(job, new Path(args[1]));

	  boolean res = job.waitForCompletion(true);
	  System.exit(res?0:1);
	  }
	}
