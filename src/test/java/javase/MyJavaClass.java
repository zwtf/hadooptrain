package javase;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 各种javase基础练习\知识\常识
 * @author wx
 *
 */

public class MyJavaClass {
	
	//测试单元  :will be run before those of the current class.No other ordering is defined. 
	@Before
	public void Setup() {
		
	}
	
	
	/**理解Long 和 long的区别:
	 * 		一个是数字,一个是实例
	 * 		一个是基本类型变量,一个是引用类型变量		
	 * @param args
	 */
	@Test
	public  void TestLong() {
		// TODO Auto-generated method stub
		long l1 = 100;
		long l2 = 100;
		Long long1 = new Long(100);
		Long long2 = new Long(100);
		
		System.out.println("l1 == l2 ?:"  + (l1 == l2 ? "true" : "false"));
		System.out.println("long1 == long2 ?:"  + (long1 == long2 ? "true" : "false"));
		System.out.println("l1 == long1 ?:"  + (l1 == long1 ? "true" : "false"));
		System.out.println("l1 == long1.intValue() ?:"  + (l1 == long1.intValue() ? "true" : "false"));
		System.out.println("long2.intValue() == long2.intValue() ?:"  + (long2.intValue() == long2.intValue() ? "true" : "false"));	
	}
	
	
	/**
	 * 理解参数绑定!!
	 * https://www.liaoxuefeng.com/wiki/1252599548343744/1260452774408320
	 */
	@Test
	public void VariableBinding() {   
		

				
        Person p = new Person();
        int n = 15; // n的值为15
        p.setAge(n); // 传入n的值
        System.out.println(p.getAge()); // 15
        n = 20; // n的值改为20
        System.out.println(p.getAge()); // 15还是20?
        
        Person3 p4 = new Person3();
        String bob = "Bob";
        p4.setName(bob); // 传入bob变量
        System.out.println(p4.getName()); // "Bob"
        bob = "Alice"; // bob改名为Alice
        System.out.println(p4.getName()); // "Bob"还是"Alice"?
        p4.setName(bob);
        System.out.println(p4.getName()); // "Bob"还是"Alice"?
        
        String s = "hello";
        String t = s;
        s = "world";
//        t = s;
        System.out.println(t); // t是"hello"还是"world"?
        
        Person2 p2 = new Person2();
        String[] fullname = new String[] {"wu","xiao"};
        p2.setName(fullname);
        System.out.println(p2.getName()); //wu xiao
        fullname[0]="liu";
        System.out.println(p2.getName()); //wu xiao 还是 liu xiao?
        
        String[] names = {"ABC", "XYZ", "zoo"};
        String s2 = names[1];
        names[1] = "cat";
        System.out.println(s2); // s是"XYZ"还是"cat"?
        
        //以下一段讨论的是方法的重载
        //方法作用类似,参数不同,叫做重载
        String s3 = "Test string";
        int n1 = s3.indexOf('t');
        int n2 = s3.indexOf("st");
        int n3 = s3.indexOf("st", 4);
        System.out.println(n1);
        System.out.println(n2);
        System.out.println(n3);
        
        Person4 ming = new Person4();
        Person4 hong = new Person4();
        ming.setName("Xiao Ming");
        // TODO: 给Person4增加重载方法setName(String, String):
        hong.setName("Xiao", "Hong");
        System.out.println(ming.getName());
        System.out.println(hong.getName());

 
	}	
	
	    class Person {
	 	    private int age;
	 	    
	 	    
	 	    public int getAge() {
	 	        return this.age;
	 	    }
	
	 	    public void setAge(int age) {
	 	        this.age = age;
	 	    }
	 	}
	
	 	class Person2 {
	 	    private String[] name;
	
	 	    public String getName() {
	 	        return this.name[0] + " " + this.name[1];
	 	    }
	
	 	    public void setName(String[] name) {
	 	        this.name = name;
	 	    }
	 	}
	
	 	class Person3 {
	 	    private String name;
	
	 	    public String getName() {
	 	        return this.name;
	 	    }
	
	 	    public void setName(String name) {
	 	        this.name = name;
	 	    }
	 	   
	 	}
	
	 	//方法重载
	 	class Person4 {
 	    private String name;
// 	    private String firstname;
// 	    private String lastname;
 	    
 	    public  String getName() {
 	        return name ;	        
 	    }
 	    
 	    public void setName(String name) {
 	        this.name = name;
 	    }
 	    
 		public void setName(String firstname, String lastname) {
 			this.name = firstname + " " + firstname;
 		}		
 	}
	
	
	
	
	
	
	
	
	//test git commit
	

	
	 	
	 	
	
	
	
	
	
	
	//测试单元  : methods declared in superclasses will be run after those of the current
	@After
	public void TearDown() {
		
	}

 }