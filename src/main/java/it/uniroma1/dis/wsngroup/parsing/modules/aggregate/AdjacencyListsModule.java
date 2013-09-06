package it.uniroma1.dis.wsngroup.parsing.modules.aggregate;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.log4j.Logger;

/**
 * @author Francesco Ficarola
 *
 */

public class AdjacencyListsModule implements GraphRepresentation {

	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	
	private Map<String, LinkedList<String>> adjacencyList;
	private String idSourceElement;
	private String idTargetElement;


	public AdjacencyListsModule(File fileInput, FileInputStream fis) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		adjacencyList = new HashMap<String, LinkedList<String>>();
		idSourceElement = "";
		idTargetElement = "";
	}


	private void packetParsing(String s, Integer startTS, Integer endTS) throws IOException {
		logger.debug(s);
		
		String currentToken = "";
		ArrayList<String> tokens = new ArrayList<String>();
		
		boolean addToken = false;
		StringTokenizer st = new StringTokenizer(s, " ");
		
		while(st.hasMoreTokens()) {
			currentToken = st.nextToken();
						
			if(currentToken.matches("\\bS\\b") || addToken) {
				addToken = true;
				tokens.add(currentToken);
			}
			
			else
				
			if(currentToken.matches("\\bC\\b") || addToken) {
				addToken = true;
				tokens.add(currentToken);
			}
		}
		
		
		// Vincolo sul timestamp
		if(tokens.size() > 1 && (Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, "")) >= startTS && 
				Integer.parseInt(tokens.get(ParsingConstants.TIMESTAMP_INDEX).replace(ParsingConstants.TIMESTAMP_PREFIX, "")) <= endTS)) {
			
			for(int i = 0; i < tokens.size(); i++) {
				
				/* 
				 * RILEVAMENTI
				 */
				
				if(tokens.get(ParsingConstants.TYPE_MESSAGE_INDEX).equals("S")) {
									
					if(tokens.get(i).contains(ParsingConstants.SOURCE_PREFIX)) {
						idSourceElement = tokens.get(i).replace(ParsingConstants.SOURCE_PREFIX, "");
						
						// Se non esiste una chiave nella HashMap uguale
						// a "idSourceElement" allora provvedo a crearla.
						if(!adjacencyList.containsKey(idSourceElement)) {
							LinkedList<String> currentList = new LinkedList<String>();
							adjacencyList.put(idSourceElement, currentList);	
						}
					}
				}
				
				else
				
				if(tokens.get(ParsingConstants.TYPE_MESSAGE_INDEX).equals("C")) {
							
					if(tokens.get(i).contains(ParsingConstants.SOURCE_PREFIX)) {
						idSourceElement = tokens.get(i).replace(ParsingConstants.SOURCE_PREFIX, "");
									
						// Se non esiste una chiave nella HashMap uguale
						// a "idSourceElement" allora provvedo a crearla.
						if(!adjacencyList.containsKey(idSourceElement)) {	
							LinkedList<String> currentList = new LinkedList<String>();
							adjacencyList.put(idSourceElement, currentList);	
						}
					}
					
					else
					
					
					/*
					 * CONTATTI DIRETTI
					 */
					
					// Espressione regolare per un modello simile a [1203(1) :
					// \\x5b = [
					// \\d+ = qualsiasi numero ripetuto da 1 a n volte
					// .* = qualsiasi carattere ripetuto da 0 a n volte
					// \\x28 = (
					// \\x29 = )
					if(tokens.get(i).matches("\\x5b\\w+.*")) {
						idTargetElement = tokens.get(i).replaceAll("\\x5b|\\x28\\w+\\x29", "");
	
						// Se non esiste gia' un contatto per il nodo rappresentato
						// dall'indice della lista, allora provvedo ad aggiungerlo.
						boolean exist = false;
						for(int j=0; j<adjacencyList.get(idSourceElement).size(); j++) {
							if(adjacencyList.get(idSourceElement).get(j).equals(idTargetElement)) {
								exist = true;
								break;
							}
						}
						
						// Se non c'e' nessuna corrispondenza (ovvero il contatto cercato
						// non esiste in lista) allora lo aggiungo alla fine della lista
						if(!exist) {
							adjacencyList.get(idSourceElement).add(idTargetElement);
							
							// Se esiste gia' una lista con indice pari a idTargetElement
							// aggiungo a tale lista il contatto idSourceElement in
							// modo da rendere perpendicolari l'insieme delle liste.
							// Viceversa, se non dovesse esistere, la creo e successivamente
							// aggiungo a tale lista il contatto idSourceElement.
							if(adjacencyList.containsKey(idTargetElement)) {
								boolean existTarget = false;
								for(int j=0; j<adjacencyList.get(idTargetElement).size(); j++) {
									if(adjacencyList.get(idTargetElement).get(j).equals(idSourceElement)) {
										existTarget = true;
										break;
									}
								}
								
								if(!existTarget) {
									adjacencyList.get(idTargetElement).add(idSourceElement);
								}
							} else {
								LinkedList<String> currentList = new LinkedList<String>();
								currentList.add(idSourceElement);
								adjacencyList.put(idTargetElement, currentList);
							}				
						}
					}				
				}
			}
		}
	}


	public void readLog(Integer startTS, Integer endTS) throws IOException {
		logger.info("Reading...");
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fis));

			String currentLine;
			while((currentLine=br.readLine()) != null) {
				if(!currentLine.trim().equalsIgnoreCase("")) {
					logger.debug(currentLine.trim());
					packetParsing(currentLine.trim(), startTS, endTS);
				}
			}
		} finally {
			if(br != null) {
				br.close();
			}
		}
	}


	public void writeFile(String outputFileName, String separator) throws IOException {
		if(adjacencyList.size() > 0) {
			logger.info("Writing...");
			
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			PrintStream ps = new PrintStream(fos);
			
			Set<String> adjacencyListSorted = new TreeSet<String>(adjacencyList.keySet());
			for(String firstElement : adjacencyListSorted) {
				ps.print(firstElement);
				if(adjacencyList.get(firstElement).size() > 0) {
					ps.print(separator);
					for(int i = 0; i < adjacencyList.get(firstElement).size(); i++) {
						Collections.sort(adjacencyList.get(firstElement));
						if(i != adjacencyList.get(firstElement).size()-1) {
							ps.print(adjacencyList.get(firstElement).get(i) + separator);
						} else {
							ps.print(adjacencyList.get(firstElement).get(i));
						}
					}
				}
				ps.println();
			}
			
			logger.info("Output file: " + f.getAbsolutePath());
			ps.close();
		}
	}
}