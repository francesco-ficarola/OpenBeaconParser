package it.uniroma1.dis.wsngroup.parsing.modules.aggregate;

import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

@Deprecated
public class AdjacencyListsModuleOld implements GraphRepresentation {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */

	private Logger logger = Logger.getLogger(this.getClass());
	
	private File fileInput;
	private FileInputStream fis;
	
	private LinkedList<LinkedList<Integer>> adjacencyList;
	private Integer indexExistList;
	private Integer indexPosition;
	private Integer idSourceElement;
	private Integer idTargetElement;


	public AdjacencyListsModuleOld(File fileInput, FileInputStream fis) {
		this.fileInput = fileInput;
		this.fis = fis;
		
		adjacencyList = new LinkedList<LinkedList<Integer>>();
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
							
							LinkedList<Integer> currentList = new LinkedList<Integer>();
							currentList.add(idSourceElement);
							
							if(!indexPosition.equals(-1)) {
								adjacencyList.add(indexPosition, currentList);
								indexExistList = indexPosition;
							} else {
								adjacencyList.add(currentList);
								indexExistList = adjacencyList.size()-1;
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
							
							LinkedList<Integer> currentList = new LinkedList<Integer>();
							currentList.add(idSourceElement);
							
							// Se nella lista generale esistono indici di liste con
							// ID minore a quello che stiamo tentando di inserire,
							// allora in indexPosition verra' memorizzato l'indice + 1
							// dell'ultimo elemento minore. Ad esempio, se abbiamo
							// delle liste con indici pari a 1110, 1112, 1113, 1115
							// e vogliamo inserire una lista con indice 1114, in
							// indexPosition verra' memorizzato 3 (ovvero 2 + 1, dove
							// 2 e' l'indice di 1113,l'ultimo ID minore di 1114)
							if(!indexPosition.equals(-1)) {
								adjacencyList.add(indexPosition, currentList);
								indexExistList = indexPosition;
							} else {
								adjacencyList.add(currentList);
								indexExistList = adjacencyList.size()-1;
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
						idTargetElement = Integer.parseInt(tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", ""));
	
						// Se non esiste gia' un contatto per il nodo rappresentato
						// dall'indice della lista, allora provvedo ad aggiungerlo.
						Integer[] outputSearchNode = searchNode(idTargetElement, adjacencyList.get(indexExistList));
						
						// Se non c'e' nessuna corrispondenza (ovvero il contatto cercato
						// non esiste in lista) allora lo aggiungo alla fine della lista
						if(outputSearchNode[0].equals(-1)) {
							
							// Se la lista sulla quale si sta effettuando la ricerca ha
							// almeno un contatto e tale contatto e' minore di quello che si
							// sta tentando di aggiungere, allora in outputSearchNode[1]
							// avro' un valore diverso da -1 che mi indichera' l'indice
							// della lista dove inserire il nuovo contatto.
							// Viceversa, se la lista non ha alcun contatto, lo aggiungo alla fine.
							if(!outputSearchNode[1].equals(-1)) {
								adjacencyList.get(indexExistList).add(outputSearchNode[1].intValue(), idTargetElement);
							} else {
								adjacencyList.get(indexExistList).add(idTargetElement);
							}
							
							
							// Se esiste gia' una lista con indice pari a idTargetElement
							// aggiungo a tale lista il contatto idSourceElement in
							// modo da rendere perpendicolari l'insieme delle liste.
							// Viceversa, se non dovesse esistere, la creo e successivamente
							// aggiungo a tale lista il contatto idSourceElement.
							Integer[] outputSearchIndexPL = searchIndexList(idTargetElement);
							Integer indexPerpendicularList = outputSearchIndexPL[0];
							Integer indexPositionPerpendicularList = outputSearchIndexPL[1];
							if(!indexPerpendicularList.equals(-1)) {
								Integer position = 1;
								for(int t = 1; t < adjacencyList.get(indexPerpendicularList).size(); t++) {
									if(idSourceElement > adjacencyList.get(indexPerpendicularList).get(t).intValue()) {
										position = t + 1;
									}
								}
								adjacencyList.get(indexPerpendicularList).add(position.intValue(), idSourceElement);
							} else {
								LinkedList<Integer> newPerpendicularList = new LinkedList<Integer>();
								newPerpendicularList.add(idTargetElement);
								newPerpendicularList.add(idSourceElement);
								
								if(!indexPositionPerpendicularList.equals(-1)) {
									adjacencyList.add(indexPositionPerpendicularList, newPerpendicularList);
								} else {
									adjacencyList.add(newPerpendicularList);
								}
							}						
						}
					}				
				}
			}
		}
	}
	
	
	private Integer[] searchIndexList(Integer value) {
		Integer indexExistList = -1;
		Integer indexPosition = -1;
		
		if(adjacencyList.size() > 0) {
			indexPosition = 0;
			
			for(int i = 0; i < adjacencyList.size(); i++) {
				
				if(adjacencyList.get(i).get(0).equals(value)) {
					indexExistList = i;
					break;
				}
				
				else
				
				if(value.intValue() > adjacencyList.get(i).get(0).intValue()) {
					indexPosition = i + 1;
				}
			}
		}
		return new Integer[]{indexExistList, indexPosition};
	}
	
	
	private Integer[] searchNode(Integer value, LinkedList<Integer> nodesList) {
		Integer indexExistNode = -1;
		Integer position = -1;
		
		// Se la lista contiene contatti oltre all'indice
		if(nodesList.size() > 1) {
			
			// Impongo la condizione di ipotesi che l'elemento che sto
			// per aggiungere sia minore di tutti quelli gia' esistenti
			// quindi debba essere aggiunto all'inizio dei contatti.
			position = 1;
			
			// Il ciclo parte da 1 perché non bisogna tener conto del
			// primo elemento della lista, il quale rappresenta l'indice.
			for(int i = 1; i < nodesList.size(); i++) {
				
				// Se trovo una corrispondenza (contatto gia' esistente)
				// ritorno la sua posizione
				if(nodesList.get(i).equals(value)) {
					indexExistNode = i;
					break;
				}
				
				else
				
				// Se "value", ovvero il valore che si sta cercando,
				// e' maggiore di un elemento nella lista, allora
				// memorizzo la sua posizione+1 in modo da aggiungerlo
				// subito dopo e far avanzare in avanti tutti gli altri.
				if(value.intValue() > nodesList.get(i).intValue()) {
						position = i + 1;
				}
			}
		}
		return new Integer[]{indexExistNode, position};
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
		if(adjacencyList.size() > 0) {
			logger.info("Writing...");
			
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			PrintStream ps = new PrintStream(fos);
			
			for(int i = 0; i < adjacencyList.size(); i++) {
				for(int j = 0; j < (adjacencyList.get(i)).size(); j++) {
					if(j != adjacencyList.get(i).size()-1) {
						ps.print(adjacencyList.get(i).get(j) + separator);
					} else {
						ps.print(adjacencyList.get(i).get(j));
					}
				}
				ps.println();
			}
			
			logger.info("Output file: " + f.getAbsolutePath());
			ps.close();
		}
	}
}