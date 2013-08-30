package it.uniroma1.dis.wsngroup.core.ml;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Francesco Ficarola
 *
 */

public class AttributeMapping {
	
	private static Map<String, Integer> SEX = new HashMap<String, Integer>();
	static {
		SEX.put("M", 0);
		SEX.put("F", 1);
	}
	
	private static Map<String, Integer> NATIONALITY = new HashMap<String, Integer>();
	static {
		NATIONALITY.put("Italy", 0);
		NATIONALITY.put("Other", 1);
	}
	
	private static Map<String, Integer> GRADE_LEVEL = new HashMap<String, Integer>();
	static {
		GRADE_LEVEL.put("Primary School", 0);
		GRADE_LEVEL.put("Mid School", 1);
		GRADE_LEVEL.put("High School", 2);
		GRADE_LEVEL.put("Bachelor", 3);
		GRADE_LEVEL.put("Master", 4);
		GRADE_LEVEL.put("Ph.D.", 5);
	}
	
	private static Map<String, Integer> OCCUPATION = new HashMap<String, Integer>();
	static {
		OCCUPATION.put("Art", 0);
		OCCUPATION.put("Building_Trade", 1);
		OCCUPATION.put("Catering", 2);
		OCCUPATION.put("Commerce", 3);
		OCCUPATION.put("Computer_Science", 4);
		OCCUPATION.put("Economics", 5);
		OCCUPATION.put("Education", 6);
		OCCUPATION.put("Electronics", 7);
		OCCUPATION.put("Finance", 8);
		OCCUPATION.put("Health_Service", 9);
		OCCUPATION.put("Journalism", 10);
		OCCUPATION.put("Law", 11);
		OCCUPATION.put("Military", 12);
		OCCUPATION.put("Politics", 13);
		OCCUPATION.put("Public_Service", 14);
		OCCUPATION.put("Student", 15);
		OCCUPATION.put("Transport", 16);
		OCCUPATION.put("Unemployed", 17);
		OCCUPATION.put("Other", 18);
	}
	
	
	public static Integer getSexValue(String key) {
		return SEX.get(key);
	}
	
	public static Integer getNationalityValue(String key) {
		return NATIONALITY.get(key);
	}
	
	public static Integer getGradeLevelValue(String key) {
		return GRADE_LEVEL.get(key);
	}
	
	public static Integer getOccupationValue(String key) {
		return OCCUPATION.get(key);
	}
	
}
