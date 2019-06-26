package hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 调用JAVA API访问HDFS目录执行文件/文件夹操作
 * @author wx
 *
 */

public class HDFSApp {

	public static final String HDFS_PATH = "hdfs://192.168.111.109:9400";

	FileSystem fileSystem = null;
	FileSystem fs = null;
	Configuration configuration = null;
	
	@Before
	public void	Setup() throws IOException, InterruptedException, URISyntaxException {
		System.out.println("HDFSApp.Setup");
		Configuration conf = new  Configuration();
//		不能这样写,这样写在方法里面相当于在方法里面重新定义了两个field
//		FileSystem fs = FileSystem.newInstance(new URI(HDFS_PATH), conf, "root");
//		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "root");
		
		//两种实例化方法,效果一样
		fs = FileSystem.newInstance(new URI(HDFS_PATH), conf, "root");
		fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "root");		
	}
	/**
	 * API创建文件夹
	 * 
	 * @throws Exception
	 */
	@Test
	public void mkdir() throws Exception {
		fileSystem.mkdirs(new Path("/hdfsapi/test"));

	}

	/*
	 * API创建文件
	 * 
	 * fileSystem.createFile(Path) 返回的是一个 private 的
	 * FileSystemDataOutputStreamBuilder,但因为类其实是private的
	 * 所以不能import，需要调用build()方法，返回一个FSDataOutputStream实例，才能使用他的流 import
	 * org.apache.hadoop.fs.FileSystem.FileSystemDataOutputStreamBuilder;
	 * 
	 */
	@Test
	public void touchFile() throws Exception {
		//build 方法:Create the FSDataOutputStream to write on the file system.
		FSDataOutputStream outputstream = fileSystem.createFile(new Path("/hdfsapi/test/testTouchFile.txt")).build();
		outputstream.writeBytes("hello wx(test cover)");
		outputstream.flush();
		outputstream.close();
	}

	/*
	 * API读取文件内容 这里发现个错误 如果不加@Test，直接run as Junits的话，会报如下错误： 
	 * java.lang.Exception: No tests found matching
	 * 
	 */
	@Test
	public void catFile() throws Exception {
		Path filePath = new Path("/hdfsapi/test/testTouchFile.txt");
		FSDataInputStream inputStream = fs.open(filePath, 1024);

//		System.out.println(inputstream.toString()); // 输出结果:org.apache.hadoop.hdfs.client.HdfsDataInputStream@8dbfffb: org.apache.hadoop.hdfs.DFSInputStream@f316aeb

//		方法1:把inputstream读到成字节数组中，在转换成字符串输出出来（空字符串会输出），
//		注意:用new String(byte[] bytes) 不要用 toString方法！
		byte[] byt = new byte[inputStream.available()];		
		System.out.println(byt.toString());  	//输出结果：[B@8dbfffb
		System.out.println(new String(byt)); 	//输出结果： 空行
 		inputStream.read(byt); 					//读取字节数组到inputStream里面
		System.out.println(new String(byt)); 	//输出结果:hello wx(test cover) -->  (正确)

//		方法2:输入流直接转输出流，输出流直接输出到屏幕上
		IOUtils.copyBytes(inputStream, System.out, 1024); // 输出结果:hello wx(test cover) --> (正确)
		
		inputStream.close();
	}

	/*
	 *
	 * 尝试用java的IO流,把一个文件的内容粘贴到另一个路径
	 * 
	 * 
	 * 
	 */
	@Test
	public void copyFile() throws Exception {
		Path filepath = new Path("/hdfsapi/test/testTouchFile.txt");
		FSDataInputStream inputstream = fs.open(filepath, 1024);

		byte[] byt = new byte[inputstream.available()];
		inputstream.read(byt);

		FSDataOutputStream outputstream = fs.createFile(new Path("/hdfsapi/test/testTouchFile_copy.txt"))
				.build();
		outputstream.write(byt);
		outputstream.flush();

		outputstream.close();
		inputstream.close();
	}

	/*
	 * API测试rename
	 */
	@Test
	public void renameFile() throws Exception {
		Path srcpath = new Path("/hdfsapi/test/testTouchFile_copy.txt");
		Path dstpath = new Path("/hdfsapi/test/testTouchFile_copy_2.txt");
		fs.rename(srcpath, dstpath);
	}

	/**
	 * 
	 * 带进度条的文件拷贝程序
	 * 
	 * @throws Exception
	 */

	@Test
	public void copyFromLocalWithProgress() throws Exception {
		Path localPath = new Path("E:\\QQfiles\\1147310964\\FaceStore.db");
		Path remotePath = new Path("/hdfsapi/test/vpn.rar");
		fs.copyFromLocalFile(localPath, remotePath);

		InputStream in = new BufferedInputStream(new FileInputStream("E:\\QQfiles\\1147310964\\FaceStore.db"));
		FSDataOutputStream output = fs.create(new Path("/hdfsapi/test/FaceStore.db"),new Progressable() {
			
//			@Override
//			public void progress() {
//				// TODO Auto-generated method stub
//				System.out.print(".");  //带进度提醒信息
										//为什么不准override这里?
//				
//			}
			public void progress() {
				// TODO Auto-generated method stub
				System.out.print(".");  //带进度提醒信息
			}
		});
		
		IOUtils.copyBytes(in, output, 4096);
	}

	@Test
	public void ListFile() throws Exception{
		FileStatus[] fileStatus = fileSystem.listStatus(new Path("/hdfsapi/test"));
		
		for (FileStatus fileStatu : fileStatus) {
			 String isDir = fileStatu.isDirectory() ? "Dir" : "File";
			 String permissionString = fileStatu.getPermission().toString();
			 String groupString = fileStatu.getGroup().toString();
			 String ownerString = fileStatu.getOwner().toString();
			 Long len = fileStatu.getLen();
			 String pathStr = fileStatu.getPath().toString();
			 System.out.println(isDir + "\t" + permissionString + "\t" + ownerString + "\t" + groupString + "\t" + len + "\t" + pathStr);
	}
		
	}
	
	
	
	@After
	public void  TearDown() throws IOException {
		//方法一:关闭所有实例
		FileSystem.closeAll();
		//方法二:相关参数置空
		fileSystem = null;
		fs = null;
		configuration = null;
		System.out.println("HDFSApp.TearDown");
	}
}
