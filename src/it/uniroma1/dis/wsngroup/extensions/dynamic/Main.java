package it.uniroma1.dis.wsngroup.extensions.dynamic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.println("Which network do you wish to build?\n1) Dis\n2) Macro");
		String choise;
		do {
			System.out.print("Please choose 1 or 2: ");
			choise = br.readLine();
		}
		while (!choise.equals("1") && !choise.equals("2"));
		
		System.out.print("Input JSON file: ");
		File inputFile = new File(br.readLine());
//		File inputFile = new File("/home/gianluca/Dropbox/Uni/TESI/data/macro/graph_2012-07-10_11.21.46_pretty_printing.json");
//		File inputFile = new File("/home/gianluca/Dropbox/Uni/TESI/data/dis/graph_dis_dynamic.json");
		
		if (!inputFile.exists()) {
			System.out.println("ERROR: invalid file");
			System.exit(-1);
		}
		
		FileReader reader = new FileReader(inputFile);
		NetworkBuilder dns = new NetworkBuilder(reader, choise);
		dns.simulateNetwork();
//		dns.printNetwork();
		dns.printNodesCount();
	}
}
