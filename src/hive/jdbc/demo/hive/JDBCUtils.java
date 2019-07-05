package demo.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 提供给不同的程序进行HIVE连接时的注册驱动,获取连接,释放资源等操作的工具类
 * @author wx
 *
 */

public class JDBCUtils {
	private static String driver = "org.apache.hive.jdbc.HiveDriver";
	//默认端口10000 注意是hive2
	private static String url = "jdbc:hive2://192.168.111.109:10000/default";  
	
	
	//通过静态块注册驱动
	//原理:java反射机制
	//由于可能会产生异常,,用try/catch包裹,并把它转换成一个运行时异常,抛出去
	static {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	//获取连接
	public static Connection getConnection() {
		try {
			 //返回一个指向hive的连接,注意需要配置用户名和密码,且这个用户名和密码就是hive-site.xml中配置的用户名密码(why?)
			return DriverManager.getConnection(url,"root","123456");
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	//释放资源
	public static void release(Connection conn, Statement st , ResultSet rs ) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				rs = null; //置为空值后,会成为建议JVM垃圾回收的对象
			}
		}
		
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				st = null; //置为空值后,会成为建议JVM垃圾回收的对象
			}
		}
		
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				conn = null; //置为空值后,会成为建议JVM垃圾回收的对象
			}
		}
	}
	
}
