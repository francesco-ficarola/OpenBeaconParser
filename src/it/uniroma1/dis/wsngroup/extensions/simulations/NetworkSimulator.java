package it.uniroma1.dis.wsngroup.extensions.simulations;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import it.uniroma1.dis.wsngroup.extensions.simulations.NetworkBuilder.JsonEntry;

/** Simulates the evolution process of a network and returns the algorithms' results */
public class NetworkSimulator {
	public static void main(String[] args) throws IOException {
		BufferedReader brIn = new BufferedReader(new InputStreamReader(System.in));
		boolean estimatePagerank = false, estimateDegree = false;
		
		System.out.println("* Which network do you wish to build?");
		
		for(Network n : Network.values())
			System.out.println(n.ordinal() + ") " + n.toString());

		String input;
		int choise;
		do {
			System.out.print("Choose one of the values above: ");
			input = brIn.readLine();
			if (Utils.isInt(input))
				choise = Integer.parseInt(input);
			else choise = -1;
		}
		while (choise < 0 || choise >= Network.values().length);
		
		Network network = null;
		for (Network n : Network.values()) {
			if (choise == n.ordinal()) {
				network = n;
				break;
			}
		}
		
		if (network == null) {
			System.out.println("Error: unsupported network");
			System.exit(-1);
		}
		
		System.out.print("* Input JSON file for the " + network.name() + " network: ");
		String inputFilePath = brIn.readLine();
		File inputFile = new File(inputFilePath);
		
		if (!inputFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		System.out.println("* Which importance measure do you want to compute?");
		
		System.out.println("0) PageRanks");
		System.out.println("1) Degree centrality");
		System.out.println("2) Both");
		
		String input2;
		int choise2;
		do {
			System.out.print("Choose one of the values above: ");
			input2 = brIn.readLine();
			if (Utils.isInt(input2))
				choise2 = Integer.parseInt(input2);
			else choise2 = -1;
		}
		while (choise2 < 0 || choise2 > 2);
		
		if (choise2 == 0)
			estimatePagerank = true;
		else if (choise2 == 1)
			estimateDegree = true;
		else if (choise2 == 2) {
			estimatePagerank = true;
			estimateDegree = true;
		}
		else System.exit(-1);
		
		String value;
		int runs = 0;
		System.out.println("* How many runs do you want to execute? (mean values will be output)");
		do {
			System.out.print("Enter a positive integer value: ");
			value = brIn.readLine();
			if (Utils.isInt(value))
				runs = Integer.parseInt(value);
			else runs = 0;
		}
		while (runs <= 0);
		
		JsonEntry[] entries = null;
		String basePath = inputFile.getParentFile().getAbsolutePath();
		
		if (runs > 1) {
			List<Node> nodes = new ArrayList<Node>();
			
			for (int i = 0; i < runs; i++) {
				System.out.println();
				System.out.println("->RUN " + (int)(i+1) + " OF " + runs + "<-");
				
				NetworkBuilder nb = null;
				
				if (i == 0) {
					nb = new NetworkBuilder(inputFile, network, estimatePagerank, estimateDegree);
					nb.simulateRealTracesNetwork();
					entries = nb.getJsonEntries();
					nodes = nb.getNodes();
				} 
				else {
					nb = new NetworkBuilder(entries, inputFile, network, estimatePagerank, estimateDegree);
					nb.simulateRealTracesNetwork();
					List<Node> temp = nb.getNodes();
					for (int j = 0; j < nodes.size(); j++) {
						if (estimatePagerank) {
							nodes.get(j).setCWPestimation((nodes.get(j).getCWPestimation() + 
									temp.get(j).getCWPestimation()));
							nodes.get(j).setCWEestimation((nodes.get(j).getCWEestimation() + 
									temp.get(j).getCWEestimation()));
						}
						
						if (estimateDegree) {
							nodes.get(j).setDUCestimation(nodes.get(j).getDUCsketch().estimateCount() 
									+ temp.get(j).getDUCsketch().estimateCount());
							nodes.get(j).setENSestimation(nodes.get(j).getENSsketch().estimateCount() 
									+ temp.get(j).getENSsketch().estimateCount());
						}
					}
				}
			}
			
			System.out.println("Computing average value for each node...");			
			for (int j = 0; j < nodes.size(); j++) {
				if (estimatePagerank) {
					nodes.get(j).setCWPestimation(nodes.get(j).getCWPestimation()/runs);
					nodes.get(j).setCWEestimation(nodes.get(j).getCWEestimation()/runs);
				}
				
				if (estimateDegree) {
					nodes.get(j).setDUCestimation(nodes.get(j).getDUCestimation()/runs);
					nodes.get(j).setENSestimation(nodes.get(j).getENSestimation()/runs);
				}
			}
			
			
			if (estimatePagerank) {
				Collections.sort(nodes, new NodesCWPComparator());
				
				StringBuffer sbCWP = new StringBuffer("#ID;\"CWP results\";In-degree;Out-degree\n");
				for (Node no :  nodes) {
					sbCWP.append(no.printCWPestimation());
				}
				
				Collections.sort(nodes, new NodesCWEComparator());
				
				StringBuffer sbCWE = new StringBuffer("#ID;\"CWE results\";In-degree;Out-degree\n");
				for (Node no :  nodes) {
					sbCWE.append(no.printCWEestimation());
				}
				
				Utils.printToCSVFile(sbCWP.toString(), basePath + "/CWP_results_x" + runs + ".csv");
				Utils.printToCSVFile(sbCWE.toString(), basePath + "/CWE_results_x" + runs + ".csv");
			}
			
			if (estimateDegree) {				
				StringBuffer sbDUC = new StringBuffer("#ID;\"DUC results\";\"distinct users\"\n");
				for (Node no :  nodes) {
					sbDUC.append(no.printDUCestimation());
				}
				
				StringBuffer sbENS = new StringBuffer("#ID;\"ENS results\";\"subnetwork size\"\n");
				for (Node no :  nodes) {
					sbENS.append(no.printENSestimation());
				}
				
				Utils.printToCSVFile(sbDUC.toString(),  basePath + "/DUC_results_x" + runs + ".csv");
				Utils.printToCSVFile(sbENS.toString(),  basePath + "/ENS_results_x" + runs + ".csv");
			}
		}
		else { // only one run
			NetworkBuilder nb = new NetworkBuilder(inputFile, network, estimatePagerank, estimateDegree);
			nb.simulateRealTracesNetwork();
			
			if (estimatePagerank) {
				Utils.printToCSVFile(nb.getCWPresults(),  basePath + "/CWP_results.csv");
				Utils.printToCSVFile(nb.getCWEresults(),  basePath + "/CWE_results.csv");
			}
			
			if (estimateDegree) {
				Utils.printToCSVFile(nb.getDUCresults(),  basePath + "/DUC_results.csv");
				Utils.printToCSVFile(nb.getENSresults(),  basePath + "/ENS_results.csv");
			}
		}
		
		System.out.println("Done!");
	}
}
