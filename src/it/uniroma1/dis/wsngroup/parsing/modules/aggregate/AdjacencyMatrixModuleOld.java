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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

@Deprecated
public class AdjacencyMatrixModuleOld implements GraphRepresentation {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	
	private LinkedList<Integer> matrixIndex;
	private LinkedList<LinkedList<Double>> adjacencyMatrix;
	private Integer contactNumber;
	private Integer indexExistList;
	private Integer indexPosition;
	private Integer idSourceElement;
	private Integer idTargetElement;

	public AdjacencyMatrixModuleOld(File fileInput, FileInputStream fis) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		matrixIndex = new LinkedList<Integer>();
		adjacencyMatrix = new LinkedList<LinkedList<Double>>();
		contactNumber = 0;
		indexExistList = -1;
		idSourceElement = -1;
		idTargetElement = -1;
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
		if(tokens.size() > 1 && (Integer.parseInt(tokens.get(1).replace("t=", "")) >= startTS &&
				Integer.parseInt(tokens.get(1).replace("t=", "")) <= endTS)) {
			
			for(int i = 0; i < tokens.size(); i++) {

				/* 
				 * RILEVAMENTI
				 */
				
				if(tokens.get(0).equals("S")) {
									
					if(tokens.get(i).contains("id=")) {
						idSourceElement = Integer.parseInt(tokens.get(i).replace("id=", ""));
						Integer outputSearchIndex[] = searchIndexList(idSourceElement);
						indexExistList = outputSearchIndex[0];
						indexPosition = outputSearchIndex[1];
						
						// Se non esiste una lista con indice (primo elemento)
						// uguale a "idSourceElement" allora provvedo a crearla.
						if(indexExistList.equals(-1)) {
							
							// Aggiungiamo alla nuova lista tanti zeri quanti sono
							// gli elementi della matrice d'adiacenza
							LinkedList<Double> currentList = new LinkedList<Double>();
							for(int t = 0; t < adjacencyMatrix.size(); t++) {
								currentList.add(new Double(0));
							}
							
							if(!indexPosition.equals(-1)) {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(indexPosition, idSourceElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(indexPosition, currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(indexPosition, new Double(0));
								}
								
								indexExistList = indexPosition;
								
							} else {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(idSourceElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(new Double(0));
								}
								
								indexExistList = adjacencyMatrix.size()-1;
								
							}
						}
					}
				}
				
				else
				
				if(tokens.get(0).equals("C")) {
					
					if(tokens.get(i).contains("id=")) {
						idSourceElement = Integer.parseInt(tokens.get(i).replace("id=", ""));
						Integer outputSearchIndex[] = searchIndexList(idSourceElement);
						indexExistList = outputSearchIndex[0];
						indexPosition = outputSearchIndex[1];
						
						// Se non esiste una lista con indice (primo elemento)
						// uguale a "idSourceElement" allora provvedo a crearla.
						if(indexExistList.equals(-1)) {
							
							// Aggiungiamo alla nuova lista tanti zeri quanti sono
							// gli elementi della matrice d'adiacenza
							LinkedList<Double> currentList = new LinkedList<Double>();
							for(int t = 0; t < adjacencyMatrix.size(); t++) {
								currentList.add(new Double(0));
							}
							
							if(!indexPosition.equals(-1)) {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(indexPosition, idSourceElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(indexPosition, currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(indexPosition, new Double(0));
								}
								
								indexExistList = indexPosition;
								
							} else {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(idSourceElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(new Double(0));
								}
								
								indexExistList = adjacencyMatrix.size()-1;
							}
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
					if(tokens.get(i).matches("\\x5b\\d+.*")) {
						contactNumber++;
						
						idTargetElement = Integer.parseInt(tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", ""));
						Integer outputSearchIndex[] = searchIndexList(idTargetElement);
						indexExistList = outputSearchIndex[0];
						indexPosition = outputSearchIndex[1];
						
						// Se non esiste una lista con indice (primo elemento)
						// uguale a "idTargetElement" allora provvedo a crearla.
						if(indexExistList.equals(-1)) {
							
							// Aggiungiamo alla nuova lista tanti zeri quanti sono
							// gli elementi della matrice d'adiacenza
							LinkedList<Double> currentList = new LinkedList<Double>();
							for(int t = 0; t < adjacencyMatrix.size(); t++) {
								currentList.add(new Double(0));
							}
							
							if(!indexPosition.equals(-1)) {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(indexPosition, idTargetElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(indexPosition, currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(indexPosition, new Double(0));
								}
								
								indexExistList = indexPosition;
								
							} else {
								// Aggiunta dell'ID nella lista degli indici
								matrixIndex.add(idTargetElement);
								
								// Aggiunta della lista di zeri, a formare una nuova riga
								adjacencyMatrix.add(currentList);
								
								// Aggiunta degli zeri per ogni riga, a formare una nuova colonna
								for(int t = 0; t < adjacencyMatrix.size(); t++) {
									adjacencyMatrix.get(t).add(new Double(0));
								}
								
								indexExistList = adjacencyMatrix.size()-1;
							}
						}
						
						int indexSource = matrixIndex.indexOf(idSourceElement);
						int indexDest = matrixIndex.indexOf(idTargetElement);
						
						Double weight = adjacencyMatrix.get(indexSource).get(indexDest) + 1;
						adjacencyMatrix.get(indexSource).set(indexDest, weight);
						adjacencyMatrix.get(indexDest).set(indexSource, weight);
					}
				}
			}
		}
	}
	
	
	private Integer[] searchIndexList(Integer value) {
		Integer indexExistList = -1;
		Integer indexPosition = -1;
		
		if(matrixIndex.size() > 0) {
			indexPosition = 0;
			
			for(int i = 0; i < matrixIndex.size(); i++) {
				
				if(matrixIndex.get(i).equals(value)) {
					indexExistList = i;
					break;
				}
				
				else
				
				if(value.intValue() > matrixIndex.get(i).intValue()) {
					indexPosition = i + 1;
				}
			}
		}
		return new Integer[]{indexExistList, indexPosition};
	}
	

	@Override
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

	@Override
	public void writeFile(String outputFileName, String separator) throws IOException {
		
		logger.info("Number of total contacts: " + contactNumber);
		
		if(adjacencyMatrix.size() > 0 && matrixIndex.size() > 0) {
			logger.info("Writing...");
			
			NumberFormat nf = NumberFormat.getInstance();
			nf.setMaximumFractionDigits(ParsingConstants.MAXIMUM_FRACTION_DIGITS);
			
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			PrintStream ps = new PrintStream(fos);
			
			
			if((matrixIndex.size() == adjacencyMatrix.size()) && 
					(adjacencyMatrix.size() == adjacencyMatrix.get(0).size())) {
				
				// Scrittura dell'indice: prima riga, un elemento per ogni colonna
				ps.print(separator);
				for(int i = 0; i < adjacencyMatrix.size(); i++) {
					if(i != adjacencyMatrix.size()-1) {
						ps.print(matrixIndex.get(i) + separator);
					} else {
						ps.print(matrixIndex.get(i));
					}
				}
				ps.println();
				
				String cellValue = "";
				for(int i = 0; i < adjacencyMatrix.size(); i++) {
					for(int j = 0; j < (adjacencyMatrix.get(i)).size(); j++) {
						
						// Trasformo 0.0 in 0
						if(adjacencyMatrix.get(i).get(j).equals(0.0)) {
							cellValue = "" + Math.round(adjacencyMatrix.get(i).get(j));
						} else {
							cellValue = "" + nf.format(adjacencyMatrix.get(i).get(j) / contactNumber);
						}
						
						if(j != adjacencyMatrix.get(i).size()-1) {
							if(j == 0) {
								ps.print(matrixIndex.get(i) + separator + cellValue + separator);
							} else {
								ps.print(cellValue + separator);
							}
						}
						
						else {
							if(j == 0) {
								ps.print(matrixIndex.get(i) + separator + cellValue);
							} else {
								ps.print(cellValue);
							}
						}
					}
					ps.println();
				}
			
			logger.info("Output file: " + f.getAbsolutePath());
			} else {
				logger.error("Matrix size mismatched!");
			}
			
			ps.close();
		}
	}

}
