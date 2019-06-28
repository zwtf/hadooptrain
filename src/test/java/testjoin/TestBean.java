package testjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



// 20190-06-28:完整覆写了一遍还是不对

/**
	 * 测试join语句
	 * 自定义bean类型,包含所有属性和一个flag字段用于判断文件是order还是product
	 * 时间:2019-06-28
	 * @author wx
	 *
	 */

//* 样例数据:
//order
//1001 P0001 200
//1002 P0001 300
//1003 P0002 400
//1004 P0003 500
//1004 P0004 20
//
//product
//P0001 XIAOMI
//P0002 IPHONE
//P0003 NOKIA
//P0005 HuaWEi
//
//最终结果:
//1001 P0001 200 XIAOMI
//1002 P0001 300 XIAOMI
//1003 P0002 400 IPHONE
//1004 P0003 500 NOKIA
//1004 P0004 20
public class TestBean implements Writable{
	int oId;
	String pId;
	int volume;
	String brand;
	String flag;
	
	public  TestBean() {
		
	}
	
	public void set(int oId,String pId,int volume,String brand,String flag) {
		this.oId = oId;
		this.pId = pId;
		this.volume = volume;
		this.brand = brand;
		this.flag = flag;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(oId);
		out.writeUTF(pId);
		out.writeInt(volume);
		out.writeUTF(brand);
		out.writeUTF(flag);
		
	}
	public void readFields(DataInput in) throws IOException {
		oId = in.readInt();
		pId = in.readUTF();
		volume = in.readInt();
		brand = in.readUTF();
		flag = in.readUTF();
		
	}
	
	public int getoId() {
		return oId;
	}
	public void setoId(int oId) {
		this.oId = oId;
	}
	public String getpId() {
		return pId;
	}
	public void setpId(String pId) {
		this.pId = pId;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getFlag() {
		return flag;
	}
	public void setFlag(String flag) {
		this.flag = flag;
	}
	
	
	public String toString() {
		return "oid:" + oId + "pid:" + pId + "volume:" + volume + "brand:" + brand + "flag:" + flag;

	}
}
