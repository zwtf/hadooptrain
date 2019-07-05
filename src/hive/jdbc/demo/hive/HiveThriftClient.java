package demo.hive;

import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;


public class HiveThriftClient {

	/**
	 * Thrift是基于socket的数据库连接,基于Thrift产生了hiveserver1和hiveserver2
	 * @param args
	 */
	
	public static void main(String[] args) throws Exception{

		//创建socket连接
		final TSocket tSocket = new TSocket("192.168.111.109", 10000);
		
		//创建一个协议,抽象类,最终由子类来实现
		final TProtocol tProtocol = new TBinaryProtocol(tSocket);
		
		//创建Hive Client
//		final HiveServer2 client = new HiveClient(tProtocol);	//未找到HiveClient类
		
		//打开socket
		tSocket.open();
		//执行HQL
//		client.execute("select...")
		//处理结果
		
		
		
	}

}
