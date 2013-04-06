package it.uniroma1.dis.wsngroup.extensions.simulations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import com.csvreader.CsvReader;

/** 
 * This class provides functionalities for evaluating sets of results with respect to
 * the real values to be measured
 */
public class PerformanceEvaluator {
	public static double topKPrecision(int k, File estFile, File exFile) throws NumberFormatException, IOException {
		Reader estR = new FileReader(estFile);
		Reader exR = new FileReader(exFile);
		
		CsvReader estReader = new CsvReader(estR, ';');
		CsvReader exReader = new CsvReader(exR, ';');
		
		LinkedList<String> estList = new LinkedList<String>();
		LinkedList<String> exList = new LinkedList<String>();
		
		int elementsRead = 0;
		while (estReader.readRecord() && elementsRead < k) {
			String[] rowValues = estReader.getValues();
			if (rowValues[0].charAt(0) == '#')
				continue;
			
			estList.add(rowValues[0]);
			elementsRead++;
		}
		
		elementsRead = 0;
		while (exReader.readRecord() && elementsRead < k) {
			String[] rowValues = exReader.getValues();
			
			exList.add(rowValues[0]);
			elementsRead++;
		}
		
		if (estList.size() < k || exList.size() < k) {
			System.out.println("ERROR: at least one of the two lists contains less than k elements");
			return -1;
		}
			
		
		int inCommon = 0;
		for (String id : exList) {
			if (estList.contains(id))
				inCommon++;
		}
		
		return (double)inCommon/k;
	}
	
	public static void evaluatePrecision(File estFile, File exFile) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String value;
		int k1;
		do {
			System.out.print("Min value of k: ");
			value = br.readLine();
			if (Utils.isInt(value))
				k1 = Integer.parseInt(value);
			else k1 = 0;
		}
		while (k1 <= 0);
		
		int k2;
		do {
			System.out.print("Max value of k: ");
			value = br.readLine();
			if (Utils.isInt(value))
				k2 = Integer.parseInt(value);
			else k2 = 0;
		}
		while (k2 <= 0 || k2 < k1);
		
		int intv;
		do {
			System.out.print("Interval: ");
			value = br.readLine();
			if (Utils.isInt(value))
				intv = Integer.parseInt(value);
			else intv = 0;
		}
		while (intv <= 0);
		
		String filename = estFile.getAbsolutePath();
		
		StringBuffer buf = new StringBuffer("#k\tprecision@k\n");
		for (int i = k1; i <= k2; i += intv) {
			double precision = topKPrecision(i, estFile, exFile);
			buf.append(i + "\t" + precision + "\n");
		}
		
		Utils.printToCSVFile(buf.toString(),  filename + ".dat");
	}
	
	public static void evaluateAbsoluteError(File estFile) throws IOException {
		Reader estR = new FileReader(estFile);
		
		CsvReader estReader = new CsvReader(estR, ';');
		
		Map<Long, Integer> map = new HashMap<Long, Integer>();
		ValueComparator bvc =  new ValueComparator(map);
        TreeMap<Long, Integer> sorted_map = new TreeMap<Long, Integer>(bvc);
		
		int elementsRead = 0;
		while (estReader.readRecord()) {
			String[] rowValues = estReader.getValues();
			if (rowValues[0].charAt(0) == '#')
				continue;

			map.put(Long.parseLong(rowValues[0]), 
					Math.abs(Integer.parseInt(rowValues[2]) - Integer.parseInt(rowValues[1])));
			elementsRead++;
		}
		
		sorted_map.putAll(map);
		
		StringBuffer buf = new StringBuffer("#ID\terror\n");
		
		for (Map.Entry<Long, Integer> entry : sorted_map.entrySet()) {
			buf.append(entry.getKey() + "\t" + entry.getValue() + "\n");
		}
		
		String filename = estFile.getAbsolutePath();
		Utils.printToCSVFile(buf.toString(),  filename + ".dat");
	}
	
	public static void main(String[] args) throws IOException {
		BufferedReader brIn = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.print("Insert file with estimated values: ");
		File estFile = new File(brIn.readLine());
		
		if (!estFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		System.out.print("Insert file with exact values: ");
		File exFile = new File(brIn.readLine());
		
		if (!estFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		evaluatePrecision(estFile, exFile);
//		evaluateAbsoluteError(estFile);
	}
}

class ValueComparator implements Comparator<Long> {
	private Map<Long, Integer> base;
	public ValueComparator(Map<Long, Integer> base) {
		this.base = base;
	 }
	
	public int compare(Long a, Long b) {
        if (base.get(a) >= base.get(b)) {
            return 1;
        } else {
            return -1;
        }
    }
}
