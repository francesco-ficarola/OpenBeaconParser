package it.uniroma1.dis.wsngroup.core.realtime.simulation;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * @author Francesco Ficarola
 */

public class RealTimeSimulation {
	public static void main (String args[]) throws java.lang.InterruptedException {

		try {
			FileInputStream fstream = new FileInputStream("../test/Sample_Real_Data.txt");
			FileOutputStream sampleFile = new FileOutputStream("../test/RealTimeTest.txt");
			PrintStream output = new PrintStream(sampleFile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;

			while ((strLine = br.readLine()) != null)   {

				output.println(strLine);
				Thread.sleep(1000);
			}

			in.close();
			output.close();
		} catch (Exception e){
			System.err.println("Error: " + e.getMessage());
		}
	}
}
