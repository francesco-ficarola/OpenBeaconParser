package it.uniroma1.dis.wsngroup.extensions.pagerank;

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


public class Main {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Input CSV file: ");
		File inputFile = new File(br.readLine());
		
		if (!inputFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		Reader reader = new FileReader(inputFile);
		System.out.print("CSV delimiter (1 char): ");
		char delimiter = br.readLine().charAt(0);
		
		StaticPageRankModule am = new StaticPageRankModule(reader, delimiter);
		LinkedList<LinkedList<Integer>> aggregatedMatrix = am.getAggregatedMatrix();
		LinkedList<LinkedList<Double>> normalizedMatrix = am.normalizeByRowSums(aggregatedMatrix);
		LinkedList<LinkedList<Double>> stochasticMatrix = am.fillZeroRows(normalizedMatrix);
		//LinkedList<LinkedList<Double>> irreducibleMatrix = am.addPerturbationMatrix(stochasticMatrix, 0.85);
		//am.printDoubleMatrix(irreducibleMatrix);
		LinkedList<Double> pageRankVector = am.computePageRank(stochasticMatrix, 0.85);
		LinkedList<String> nodeIDs = am.getNodeIDs();
		
		HashMap<String, Double> pageRankMap = new HashMap<String, Double>(); 
		ValueComparator vc =  new ValueComparator(pageRankMap);
        TreeMap<String,Double> sortedPageRankMap = new TreeMap<String,Double>(vc);
        
		double sum = 0;
		for (int i = 0; i < pageRankVector.size(); i++) {
			sum += pageRankVector.get(i);
			pageRankMap.put(nodeIDs.get(i), pageRankVector.get(i));
		}
		
		sortedPageRankMap.putAll(pageRankMap);
		
		System.out.println("#\tNodeID\tPageRank");
		int index = 1;
		for(Map.Entry<String,Double> entry : sortedPageRankMap.entrySet()) {
			String key = entry.getKey();
			Double value = entry.getValue();
			System.out.println(index + "\t" + key + "\t" + value);
			index++;
		}
		
		System.out.println("Total sum (must be 1): "+sum);
	}
}

class ValueComparator implements Comparator<String> {

    Map<String, Double> base;
    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}