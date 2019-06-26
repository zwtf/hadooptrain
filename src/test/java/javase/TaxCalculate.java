package javase;

/**
	 * 理解多态:
	 * 		针对某个类型的方法调用，其真正执行的方法取决于运行时期实际类型的方法
	 * 
			目标：通过多态，实现让getTax()方法对三种不同工资来源实行不同的纳税策略
			
			具体做法:让不同的收入来源类继承income大类，
			 根据自己的纳税策略去覆写(Overrid)方法(这里注意区分覆写和重载！)， 		
			 在实际调用时，让不同收入来源的实例分别调用自己类下的getTax()方法，实现只调用getTax()一个方法，
			 完成不同的纳税策略,这就是多态
			 
			 好处:复用代码,便于拓展,
			 	(拓展的时候只需要加新的子类和改一下totalTax方法就行了)
	 * @author wx
	 *
	 */

public class TaxCalculate {
	
	   public static void main(String[] args) {
	        // 给一个有普通收入、工资收入和享受国务院特殊津贴的小伙伴算税:
	        Income[] incomes = new Income[] {
	            new Income(3000),
	            new Salary(7500),
	            new StateCouncilSpecialAllowance(15000)
	        };
//	        System.out.println( incomes[1].income); //7500.0
	        System.out.println(totalTax(incomes));
	    }

	    public static double totalTax(Income... incomes) {
	        double total = 0;
	        for (Income income: incomes) {
	            total = total + income.getTax();
	        }
	        return total;
	    }
	}
	
	//收入父类,指代所有类型的收入
	class Income {
	    protected double income;
	    
//	    public  Income() {
//	    	this.income = 7500;
//	    }
	    
	    public Income(double income) {
	        this.income = income;
	    }

	    public double getTax() {
	        return income * 0.1; // 税率10%
	    }
	}
	
			//工资类收入
			class Salary extends Income {
				
			    public Salary(double income) {
		//	    	super();
			        super(income);
			    }
		
			    @Override
			    public double getTax() {
			        if (income <= 5000) {
			            return 0;
			        }
			        return (income - 5000) * 0.2;
			    }
			}

			//国税局免税类收入
			class StateCouncilSpecialAllowance extends Income {
			    public StateCouncilSpecialAllowance(double income) {
			        super(income);
			    }
		
			    @Override
			    public double getTax() {
			        return 0;
			    }
}
