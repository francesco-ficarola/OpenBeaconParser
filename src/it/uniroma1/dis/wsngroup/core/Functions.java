package it.uniroma1.dis.wsngroup.core;

import java.util.List;

public class Functions {
	
	public static boolean isInteger( String input )  
	{  
	   try  
	   {  
	      Integer.parseInt(input);  
	      return true;  
	   }  
	   catch(NumberFormatException e)  
	   {  
	      return false;
	   }  
	}
	
	public static double average(List<Integer> list) {
		int sum = -1;
		
		if(list != null) {
			for(int i = 0; i < list.size(); i++) {
				sum += list.get(i);
			}
		}
		
		return (double)sum / (double)list.size();
	}
	
	public static int max(List<Integer> list) {
		int max = -1;
		
		if(list != null) {
			max = list.get(0);
			for(int i = 0; i < list.size(); i++) {
				if(max < list.get(i)) {
					max = list.get(i);
				}
			}
		}
		
		return max;
	}
	
	public static int min(List<Integer> list) {
		int min = -1;
		
		if(list != null) {
			min = list.get(0);
			for(int i = 0; i < list.size(); i++) {
				if(min > list.get(i)) {
					min = list.get(i);
				}
			}
		}
		
		return min;
	}
	
}
