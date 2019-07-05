package javatrain;

import java.util.Iterator;

/*
 * i / 10 且 aMethod(0) 时: 输出结果:finally finished
 *  10 / i 且 aMethod(0) 时: 输出结果:finally exception in main finished
 *  exception in a Method 一直不会输出 ,怎么能让它输出呢?
 *  
 */
public class Test1 {

	public static int aMethod(int i) throws Exception{
		try {
			return  10 / i ;
		} catch (Exception ex) {
			throw new Exception("exception in a Method");
		}finally {
			System.out.printf("finally");
		}
	}
	
	public static void main(String[] args) {
		try {
			aMethod(0);
		} catch (Exception exception) {
			System.out.println("exception in main");
		}finally {
			System.out.printf("finished");
		}

	}

	
	
}
