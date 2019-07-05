package demo.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * 测试hive的自定义函数
 * @author wx
 *
 */
public class HiveCustomUDF  extends UDF{

	public Text evaluate(Text a, Text b) {
		return new Text(a.toString() + "*****" + b.toString());
	}
}
