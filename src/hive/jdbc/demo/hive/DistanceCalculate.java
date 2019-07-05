package demo.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * hive自定义函数UDF
 * 功能:依次输出两个点的经纬度，输出两点的距离
 * @author wx
 *
 */

public class DistanceCalculate extends UDF{

	private static double EARTH_RADIUS = 6378.137;
	 
	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}
	
	//用DoubleWritable作为输入输出，报错：
		//FAILED: SemanticException [Error 10014]: Line 1:34 Wrong arguments '117.315575': No matching method for class demo.hive.DistanceCalculate with (double, double, double, double).Possible choices: _FUNC_(struct<value:double>, struct<value:double>, struct<value:double>, struct<value:double>)
//	public DoubleWritable evaluate(DoubleWritable lt1,DoubleWritable lg1,DoubleWritable lt2,DoubleWritable lg2) {
//		
//		double lat1 = Double.parseDouble(lt1.toString());
//		double lng1 = Double.parseDouble(lg1.toString());
//		double lat2 = Double.parseDouble(lt2.toString());
//		double lng2 = Double.parseDouble(lg2.toString());
//	
//
//
//		double radLat1 = rad(lat1);
//		double radLat2 = rad(lat2);
//		double a = radLat1 - radLat2;
//		double b = rad(lng1) - rad(lng2);
//		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
//				+ Math.cos(radLat1) * Math.cos(radLat2)
//				* Math.pow(Math.sin(b / 2), 2)));
//		s = s * EARTH_RADIUS;
//		s = Math.round(s * 10000d) / 10000d;
//		s = s * 1000;
//		return new DoubleWritable(s);
////		return null;
//		
//
//		
//	}
	
	public Text evaluate(Text lt1,Text lg1,Text lt2,Text lg2) {
		
		double lat1 = Double.parseDouble(lt1.toString());
		double lng1 = Double.parseDouble(lg1.toString());
		double lat2 = Double.parseDouble(lt2.toString());
		double lng2 = Double.parseDouble(lg2.toString());
	


		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 10000d) / 10000d;
		s = s * 1000;
		String Distance = String.valueOf(s);
		return new Text(Distance);
//		return null;
		

		
	}

}
