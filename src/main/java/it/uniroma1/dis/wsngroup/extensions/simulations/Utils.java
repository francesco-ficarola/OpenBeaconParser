package it.uniroma1.dis.wsngroup.extensions.simulations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

/** Utilities **/
public class Utils {
	public static void printToCSVFile(String toWrite, String outputFilePath) throws FileNotFoundException {
		File f = new File(outputFilePath);
		FileOutputStream fos = new FileOutputStream(f, false);
		PrintStream ps = new PrintStream(fos);
		
		System.out.println("Writing results on file...");

		ps.println(toWrite);
		ps.close();
	}
	
	public static boolean isInt(String str) {
		if (str == null)
			return false;
		
		try {
			int num = Integer.parseInt(str);
		}
		catch(NumberFormatException e) { 
			return false;
		}
		
		return true;
	}
}
