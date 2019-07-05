package demo.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * 具体的查询类,用到了自己写的JDBCUtils类
 * 
 */

public class HiveJDBCDemo {

	public static void main(String[] args) {
		//定义连接和Statement,Statement 用来指明我们要做什么样的操作
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		String sql = "select * from customer";
		try {
			//获取连接
			conn = JDBCUtils.getConnection();
			//创建运行环境
			st = conn.createStatement();
			//运行HQL语句,得到结果集
			rs = st.executeQuery(sql);
			//处理数据
			while(rs.next()) {
				String id = rs.getString(1);
				String name = rs.getString(2);
				System.out.println(id + " , " + name);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			JDBCUtils.release(conn, st, rs);
		}

	}

}
