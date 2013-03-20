package it.uniroma1.dis.wsngroup.extensions.pagerank;

import java.io.IOException;
import java.io.Reader;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.LinkedList;

import com.csvreader.CsvReader;

public class StaticPageRankModule {
	private Reader reader;
	private char delimiter;
	private CsvReader csvReader;
	private LinkedList<LinkedList<Integer>> aggregatedMatrix;
	private LinkedList<String> nodeIDs;
	private NumberFormat nf;
	
	public StaticPageRankModule(Reader r, char del) throws IOException {
		reader = r;
		delimiter = del;
		
		csvReader = new CsvReader(reader, delimiter);
		aggregatedMatrix = new LinkedList<LinkedList<Integer>>();
		nodeIDs = new LinkedList<String>();
		
		if (!csvReader.readRecord()) {
			System.out.println("ERROR: the file is empty");
			System.exit(-1);
		}
		
		String[] firstRow = csvReader.getValues(); //in the first row there are only node IDs
		for (int i = 1; i < firstRow.length; i++)
			nodeIDs.add(firstRow[i]);
		int n = firstRow.length - 1;
		
		int countRows = 0;
		while (csvReader.readRecord()) {
			String[] rowValues = csvReader.getValues();
			LinkedList<Integer> row = new LinkedList<Integer>();
			
			if (rowValues.length != n + 1) {
				System.out.println("ERROR: not all rows have the same number of elements");
				System.exit(-1);
			}
			
			for (int i = 1; i < rowValues.length; i++) //we ignore the first element of each row (it's an ID)
				row.add(Integer.parseInt(rowValues[i]));
			
			aggregatedMatrix.add(row);
			countRows++;
		}
		
		if (countRows != n) {
			System.out.println("ERROR: the matrix is not square");
			System.exit(-1);
		}
		
		nf = NumberFormat.getInstance();
		nf.setMaximumFractionDigits(5);
	}
	
	// It normalizes the matrix such that every row sums to either 1 or 0
	public LinkedList<LinkedList<Double>> normalizeByRowSums(LinkedList<LinkedList<Integer>> inputMatrix) throws IOException {
		LinkedList<LinkedList<Double>> normalizedMatrix = new LinkedList<LinkedList<Double>>();
		Iterator<LinkedList<Integer>> it = inputMatrix.iterator();

		while (it.hasNext()) {
			LinkedList<Integer> row = it.next();

			int rowSum = 0;
			for (int i = 0; i < row.size(); i++)
				rowSum += row.get(i);

			Iterator<Integer> itRow = row.iterator();
			LinkedList<Double> newRow = new LinkedList<Double>();
			while (itRow.hasNext()) {
				int element = itRow.next();
		
				if (rowSum != 0) {
					double newElement = (double)element/rowSum;
					newRow.add(newElement);
				}
				else newRow.add(0.0);
			}
			
			normalizedMatrix.add(newRow);
		}
		
		return normalizedMatrix;
	}
	
	// It replaces elements in zero rows with 1/n
	public LinkedList<LinkedList<Double>> fillZeroRows(LinkedList<LinkedList<Double>> inputMatrix) {
		LinkedList<LinkedList<Double>> stochasticMatrix = new LinkedList<LinkedList<Double>>();
		double newZeroElementsValue = (double)1/inputMatrix.size();
		Iterator<LinkedList<Double>> it = inputMatrix.iterator();

		while (it.hasNext()) {
			LinkedList<Double> row = it.next();
			boolean zeroRow = true;

			for (int i = 0; i < row.size(); i++) {
				if (row.get(i) > 0) {
					zeroRow = false;
					break;
				}
			}
			
			Iterator<Double> itRow = row.iterator();
			LinkedList<Double> newRow = new LinkedList<Double>();

			if (!zeroRow) {
				while (itRow.hasNext()) {
					double element = itRow.next();
					newRow.add(element);
				}
			}
			else {
				while (itRow.hasNext()) {
					newRow.add(newZeroElementsValue);
					itRow.next();
				}
			}
			
			stochasticMatrix.add(newRow);
		}
		
		return stochasticMatrix;
	}
	
	// P'' = alpha*P' + (1-alpha)*E
	public LinkedList<LinkedList<Double>> addPerturbationMatrix(LinkedList<LinkedList<Double>> stochasticMatrix, double alpha) {
		LinkedList<LinkedList<Double>> irreducibleMatrix = new LinkedList<LinkedList<Double>>();
		LinkedList<LinkedList<Double>> perturbationMatrix = new LinkedList<LinkedList<Double>>();
		double newZeroElementsValue = (double)1/stochasticMatrix.size();
		int n = stochasticMatrix.size();
		
		//building perturbation matrix as eeT/n
		for (int i = 0; i < n; i++) {
			LinkedList<Double> rowP = new LinkedList<Double>();
			for (int j = 0; j < n; j++)
				rowP.add(newZeroElementsValue);
			
			perturbationMatrix.add(rowP);
		}
		
		for (int k = 0; k < n; k++) {
			LinkedList<Double> row = new LinkedList<Double>();
			for (int z = 0; z < n; z++) {
				double element = alpha*stochasticMatrix.get(k).get(z) + (1-alpha)*perturbationMatrix.get(k).get(z);
				row.add(element);
			}
			irreducibleMatrix.add(row);
		}
		
		return irreducibleMatrix;
	}
	
	// pi(k+1) = alpha * pi(k) * P' + (1-alpha) * v
	public LinkedList<Double> computePageRank(LinkedList<LinkedList<Double>> stochasticMatrix, double alpha) {
		LinkedList<Double> pi = new LinkedList<Double>();
		LinkedList<Double> v = new LinkedList<Double>(); //personalization vector
		int n = stochasticMatrix.size();
		double initValue = (double)1/n;
		
		//Initialization: pi(0) = v = eT/n
		for (int i = 0; i < n; i++) {
			pi.add(initValue);
			v.add(initValue);
		}

		for (int i = 0; i < 250; i++) { //how many iterations of the power method?
			LinkedList<Double> temp = new LinkedList<Double>();
			
			for (int j = 0; j < n; j++) { //foreach element of pi
				double sum = 0;
				for (int k = 0; k < n; k++) {
					sum += pi.get(k) * stochasticMatrix.get(k).get(j);
				}
				
				double elem = alpha*sum + (1-alpha)*v.get(j);
				temp.add(elem);
			}
			pi = temp;
		}
		
		return pi;
	}
	
	public void printIntegerMatrix(LinkedList<LinkedList<Integer>> matrix) {
		Iterator<LinkedList<Integer>> it = matrix.iterator();
		
		while (it.hasNext()) {
			LinkedList<Integer> row = it.next();
			Iterator<Integer> itRow = row.iterator();
			while (itRow.hasNext())
				System.out.print(itRow.next() + " ");
			System.out.println("");
		}
	}
	
	public void printDoubleMatrix(LinkedList<LinkedList<Double>> matrix) {
		Iterator<LinkedList<Double>> it = matrix.iterator();
		
		while (it.hasNext()) {
			LinkedList<Double> row = it.next();
			Iterator<Double> itRow = row.iterator();
			while (itRow.hasNext())
				System.out.print(nf.format(itRow.next()) + " ");
			System.out.println("");
		}
	}
	
	public LinkedList<LinkedList<Integer>> getAggregatedMatrix() {
		return aggregatedMatrix;
	}
	
	public LinkedList<String> getNodeIDs() {
		return nodeIDs;
	}
}
