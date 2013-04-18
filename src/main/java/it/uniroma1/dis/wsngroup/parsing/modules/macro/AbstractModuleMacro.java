package it.uniroma1.dis.wsngroup.parsing.modules.macro;

import it.uniroma1.dis.wsngroup.constants.DBConstants;
import it.uniroma1.dis.wsngroup.db.DBObject;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;



public abstract class AbstractModuleMacro implements GraphRepresentation {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */

	protected Logger logger = Logger.getLogger(this.getClass());
	
	protected File fileInput;
	private FileInputStream fis;
	private boolean createVL;
	private ArrayList<Visitor> usersDB;
	protected List<TimestampObject> tsObjectList;
	private int indexObjects;
	private String previousTimestamp;
	
	public AbstractModuleMacro(File fileInput, FileInputStream fis, boolean createVL) {
		this.fileInput = fileInput;
		this.fis = fis;
		this.createVL = createVL;
		tsObjectList = new LinkedList<TimestampObject>();
		indexObjects = 0;
		previousTimestamp = "";
		
		// Lettura DB e memorizzazione in un ArrayList di tutti i visitatori
		usersDB = readDB();
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
		
		if(tokens.size() > 1) {
			String currentTimestamp = tokens.get(1).replace("t=", "");
			
			// Vincolo sul timestamp
			if(Integer.parseInt(currentTimestamp) >= startTS && Integer.parseInt(currentTimestamp) <= endTS) {
				
				// Gestisci il pacchetto corrente (tags e contatti REALI)
				managePacket(tokens, currentTimestamp);
				
				// Crea contatti VIRTUALI per rendere il grafo meno sparso.
				if(createVL) {
					createVirtualEdges();
				}
			}
		}
	}


	private void createVirtualEdges() {
		TimestampObject lastTimestampObject = tsObjectList.get(indexObjects-1);
		if (lastTimestampObject.getVisitors().size() > 1) {
			ArrayList<Visitor> visitors = lastTimestampObject.getVisitors();
			for(int i = 0; i < visitors.size(); i++) {
				Visitor sourceTag = visitors.get(i);
				for(int j = 0; j < visitors.size(); j++) {
					if(sourceTag.getPersonid() != visitors.get(j).getPersonid()) {
						Visitor destTag = visitors.get(j);
						
						// Controllo se l'arco è già presente nell'oggetto
						boolean existingEdge = false;
						Integer source = sourceTag.getPersonid();
						Integer target = destTag.getPersonid();
						for(int k = 0; k < lastTimestampObject.getEdges().size(); k++) {
							if((source.equals(lastTimestampObject.getEdges().get(k).getSourceTarget().get(0)) &&
								target.equals(lastTimestampObject.getEdges().get(k).getSourceTarget().get(1)))
								||
								(target.equals(lastTimestampObject.getEdges().get(k).getSourceTarget().get(0)) &&
								source.equals(lastTimestampObject.getEdges().get(k).getSourceTarget().get(1)))) {
								existingEdge = true;
								break;
							}
						}
						
						// Se il contatto non esiste, allora lo aggiungo se e solo se
						// nella loro lista dei reader c'è almeno un reader in comune
						if(!existingEdge) {
							boolean commonReader = false;
							
							for(int k = 0; k < sourceTag.getReaderIDs().size(); k++) {
								for(int t = 0; t < destTag.getReaderIDs().size(); t++) {
									if(sourceTag.getReaderIDs().get(k).equals(destTag.getReaderIDs().get(t))) {
										commonReader = true;
										break;
									}
								}
							}
							
							// Se tra i nodi sorgente e destinazione esiste almeno
							// un reader in comune allora creo tra loro un contatto virtuale.
							if(commonReader) {
								ArrayList<Integer> edgeAttr = new ArrayList<Integer>();
								edgeAttr.add(source);
								edgeAttr.add(target);
								
								//TODO implementare l'inserimento della potenza
								int power = 0;
								
								lastTimestampObject.getEdges().add(new Edge(edgeAttr, power));
							}
						}
					}
				}
			}
		}
	}


	private void managePacket(ArrayList<String> tokens, String currentTimestamp) {
		// Se il timestamp corrente è diverso dal precedente si crea
		// un nuovo oggetto TimestampObject per memorizzare tutto il necessario
		if(!currentTimestamp.equals(previousTimestamp)) {
			
			// Controllo se il sequence number è già presente nell'oggetto
			// precedente. In caso positivo ciò indica che si tratta di un
			// pacchetto visto da un differente reader troppo tardi, dunque
			// non verrà creato un nuovo oggetto Timestamp, piuttosto si aggiungerà
			// il reader (se già non presente) nella lista del precedente oggetto.
			int existingSeq = -1;
			TimestampObject prevTimestampObject = null;
			if(tsObjectList.size() > 0) {
				prevTimestampObject = tsObjectList.get(indexObjects-1);
				String seqNumber = tokens.get(5).replace("seq=", "");
				Integer sourceTag = Integer.parseInt(tokens.get(3).replace("id=", ""));
				for(int i = 0; i < prevTimestampObject.getSeqNumbers().size(); i++) {
					if(seqNumber.equals(prevTimestampObject.getSeqNumbers().get(i).getSeqNumber()) &&
							sourceTag.equals(prevTimestampObject.getSeqNumbers().get(i).getTagID())) {
						existingSeq = i;
						break;
					}
				}
			}
			

			if(existingSeq != -1) {
				// Caso in cui dovesse già esser presente il corrente sequence number
				// nell'oggetto creato in precedenza.
				if(prevTimestampObject != null) {
					manageDuplicate(tokens, prevTimestampObject, true);
				} else {
					logger.error("prevTimestampObject is null");
				}
				
			} else {
				// Sequence number non presente nell'oggetto precedente, quindi
				// posso creare un nuovo oggetto Timestamp con il nuovo timestamp.
				newTimestampObject(tokens, currentTimestamp);
			}

		} else {
			// Se il timestamp del pacchetto corrente è lo stesso del precedente
			// verifico che tale pacchetto non sia un duplicato controllando
			// i seqNumber dell'oggetto al timestamp precedente.
			
			boolean duplicate = false;
			TimestampObject prevComparingTimestampObject = null;
			if(tsObjectList.size() > 0) {
				if(currentTimestamp.equals(tsObjectList.get(indexObjects-1).getTimestamp())) {
					// Caso in cui il timestamp corrente corrisponde con quello
					// dell'ultimo oggetto creato. Ciò indica che con questo
					// timestamp è stato già creato un oggetto, di conseguenza
					// è necessario andare indietro di 2 con l'indice per pescare
					// l'oggetto del timestamp precedente, e non del corrente.
					if(tsObjectList.size() > 1) {
						prevComparingTimestampObject = tsObjectList.get(indexObjects-2);
					}
				}
				else {
					// Caso in cui il timestamp corrente non corrisponde con quello
					// dell'ultimo oggetto creato. Ciò indica che con questo
					// timestamp ancora non è stato creato alcun oggetto, di
					// conseguenza per pescare l'oggetto relativo al timestamp
					// precedente è sufficiente utilizzare "indexObjects-1".
					prevComparingTimestampObject = tsObjectList.get(indexObjects-1);
				}
				
				if(prevComparingTimestampObject != null) {
					String seqNumber = tokens.get(5).replace("seq=", "");
					Integer sourceTag = Integer.parseInt(tokens.get(3).replace("id=", ""));
					for(int i = 0; i < prevComparingTimestampObject.getSeqNumbers().size(); i++) {
						if(seqNumber.equals(prevComparingTimestampObject.getSeqNumbers().get(i).getSeqNumber()) &&
								sourceTag.equals(prevComparingTimestampObject.getSeqNumbers().get(i).getTagID())) {
							duplicate = true;
							break;
						}
					}
				}
			}
			
			if(duplicate) {
				// Caso in cui il seqNumber del pacchetto corrente è stato
				// trovato nell'oggetto con indice "indexTimestamp-1" (duplicate).
				if(prevComparingTimestampObject != null) {
					manageDuplicate(tokens, prevComparingTimestampObject, false);
				} else {
					logger.error("prevComparingTimestampObject is null");
				}
			
			} else {
				// Sequence number non presente nell'oggetto precedente (no duplicate).
				// Controllo se a questo timestamp è già stato creato un nuovo oggetto.
				if(currentTimestamp.equals(tsObjectList.get(indexObjects-1).getTimestamp())) {
					// Caso in cui è stato già creato, allora lo aggiorno.
					managePrevObject(tokens, currentTimestamp);
				} else {
					// Caso in cui non è ancora presente un oggetto con
					// il timestamp corrente, di conseguenza lo creo.
					newTimestampObject(tokens, currentTimestamp);
				}
			}
		}
		
		// Memorizzazione del timestamp corrente in previousTimestamp
		// in modo da poter comparare questo valore con il successivo
		previousTimestamp = currentTimestamp;
	}


	private void managePrevObject(ArrayList<String> tokens, String currentTimestamp) {
		
		Integer idPersonSourceNode = -1;
		Integer idPersonTargetNode = -1;
		
		for(int i = 0; i < tokens.size(); i++) {
			
			/* 
			 * TOKEN "id="
			 */
			if(tokens.get(i).contains("id=")) {
				String idBadgeSourceNode = tokens.get(i).replace("id=", "");
				int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeSourceNode), currentTimestamp);
				if(indexNodeDB >= 0) {
										
					Visitor visitor = new Visitor(usersDB.get(indexNodeDB));
					idPersonSourceNode = visitor.getPersonid();
					
					// Controllo se il visitatore è già presente nell'oggetto
					int existing = -1;
					for(int k = 0; k < tsObjectList.get(indexObjects-1).getVisitors().size(); k++) {
						if(idPersonSourceNode.equals(Integer.valueOf(tsObjectList.get(indexObjects-1).getVisitors().get(k).getPersonid()))) {
							existing = k;
							break;
						}
					}
					
					// Se non esiste, allora lo aggiungo
					if(existing == -1) {
						ArrayList<String> readerIDs = new ArrayList<String>();
						readerIDs.add(tokens.get(2).replace("ip=", ""));
						visitor.setReaderIDs(readerIDs);
						tsObjectList.get(indexObjects-1).getVisitors().add(visitor);
					} else {
						// Se già esiste devo aggiornare solo la lista dei readerIDs se il corrente non è già presente
						String readerID = tokens.get(2).replace("ip=", "");
						int existingReader = -1;
						for(int z = 0; z < tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().size(); z++) {
							if(readerID.equals(tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().get(z))) {
								existingReader = z;
								break;
							}
						}
						
						if(existingReader == -1) {
							tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().add(readerID);
						}
					}
				} else {
					logger.error("No match for the tag " + idBadgeSourceNode + " for TS " + currentTimestamp);
				}
			}
			
			else
				
			/*
			 * TOKEN "ip=*"
			 */
			if(tokens.get(i).contains("ip=")) {
				String idReader = tokens.get(i).replace("ip=", "");
				
				// Controllo se il reader è già presente nell'oggetto
				int existing = -1;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getReaders().size(); k++) {
					if(idReader.equals(tsObjectList.get(indexObjects-1).getReaders().get(k).getReaderID())) {
						existing = k;
						break;
					}
				}
				
				// Se non esiste, allora lo aggiungo
				if(existing == -1) {
					tsObjectList.get(indexObjects-1).getReaders().
									add(new Reader(idReader, "compareRoom"));
				}
			}
			
			else

			/*
			 * TOKEN "seq=*"
			 */
			if(tokens.get(i).contains("seq=")) {
				String seqNumber = tokens.get(i).replace("seq=", "");
				Integer sourceTag = Integer.parseInt(tokens.get(3).replace("id=", ""));
				
				// Controllo se il sequence number è già presente nell'array
				int existing = -1;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getSeqNumbers().size(); k++) {
					if(seqNumber.equals(tsObjectList.get(indexObjects-1).getSeqNumbers().get(k).getSeqNumber()) &&
							sourceTag.equals(tsObjectList.get(indexObjects-1).getSeqNumbers().get(k).getTagID())) {
						existing = k;
						break;
					}
				}
				
				// Se non esiste, allora lo aggiungo
				if(existing == -1) {
					tsObjectList.get(indexObjects-1).getSeqNumbers().add(new Seq(seqNumber, sourceTag));
				}
			}
			
			/*
			 * TOKENS "C" && [xxxx (x)
			 */
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\d+.*")) {
				String idBadgeTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", "");
				
				int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeTargetNode), currentTimestamp);
				if(indexNodeDB >= 0) {
									
					Visitor visitor = new Visitor(usersDB.get(indexNodeDB));
					idPersonTargetNode = visitor.getPersonid();
					
					// Controllo se il visitatore è già presente nell'oggetto
					int existing = -1;
					for(int k = 0; k < tsObjectList.get(indexObjects-1).getVisitors().size(); k++) {
						if(idPersonTargetNode.equals(Integer.valueOf(tsObjectList.get(indexObjects-1).getVisitors().get(k).getPersonid()))) {
							existing = k;
							break;
						}
					}
					
					// Se non esiste, allora lo aggiungo
					if(existing == -1) {
						ArrayList<String> readerIDs = new ArrayList<String>();
						readerIDs.add(tokens.get(2).replace("ip=", ""));
						visitor.setReaderIDs(readerIDs);
						tsObjectList.get(indexObjects-1).getVisitors().add(visitor);
					} else {
						// Se già esiste devo aggiornare solo la lista dei readerIDs se il corrente non è già presente
						String readerID = tokens.get(2).replace("ip=", "");
						int existingReader = -1;
						for(int z = 0; z < tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().size(); z++) {
							if(readerID.equals(tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().get(z))) {
								existingReader = z;
								break;
							}
						}
						
						if(existingReader == -1) {
							tsObjectList.get(indexObjects-1).getVisitors().get(existing).getReaderIDs().add(readerID);
						}
					}
				} else {
					logger.error("No match for the tag " + idBadgeTargetNode + " for TS " + currentTimestamp);
				}
				
				// Controllo se l'arco è già presente nell'oggetto
				boolean existingEdge = false;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getEdges().size(); k++) {
					if((idPersonSourceNode.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(0)) &&
						idPersonTargetNode.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(1)))
						||
						(idPersonTargetNode.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(0)) &&
						idPersonSourceNode.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(1)))) {
						existingEdge = true;
						break;
					}
				}
				
				// Se non esiste ed i visitatori sono stati trovati nel DB, allora lo aggiungo
				if(!existingEdge) {
					if(!idPersonSourceNode.equals(-1) && !idPersonTargetNode.equals(-1)) {
						ArrayList<Integer> edgeAttr = new ArrayList<Integer>();
						edgeAttr.add(idPersonSourceNode);
						edgeAttr.add(idPersonTargetNode);
						
						//TODO implementare l'inserimento della potenza
						int power = 0;
						
						tsObjectList.get(indexObjects-1).getEdges().add(new Edge(edgeAttr, power));
					} else {
						logger.error("No edge to build: idPersonSourceNode (" + idPersonSourceNode+ ") or idPersonTargetNode (" + idPersonTargetNode + ") wasn't found in DB.");
					}
				} 
			}
		}
	}


	private void manageDuplicate(ArrayList<String> tokens, TimestampObject prevComparingTimestampObject, boolean firstElementDuplicate) {
		// Poiché è un duplicato allora aggiornerò soltanto il reader corrente
		// (se non già presente) nella lista generale ed in quelle dei tag.
		if(firstElementDuplicate) {
			logger.info("DUPLICATE (1st)" + " --> timestamp:" + tokens.get(1).replace("t=", "") + " id:" + tokens.get(3).replace("id=", "") + " seq:" + tokens.get(5).replace("seq=", ""));
		} else {
			logger.info("DUPLICATE" + " --> timestamp:" + tokens.get(1).replace("t=", "") + " id:" + tokens.get(3).replace("id=", "") + " seq:" + tokens.get(5).replace("seq=", ""));
		}
		
		// Controllo se il reader corrente è presente nella
		// lista dei reader dell'oggetto Timestamp precedente.
		String idReader = tokens.get(2).replace("ip=", "");
		int existingReader = -1;
		for(int i = 0; i < prevComparingTimestampObject.getReaders().size(); i++) {
			if(idReader.equals(prevComparingTimestampObject.getReaders().get(i).getReaderID())) {
				existingReader = i;
				break;
			}
		}
		
		// Se il reader non è presente lo aggiungo nella lista generale dei reader.
		if(existingReader == -1) {
			
			// Inclusione nella lista generale dei reader.
			prevComparingTimestampObject.getReaders().add(new Reader(idReader, "compareRoom"));						
		}
		
		// Provvedo ad aggiungere il reader nei tag con cui ha comunicato
		// qualora già non lo posseggono.
		
		// Lista dei visitatori precedenti
		ArrayList<Visitor> prevVisitors = prevComparingTimestampObject.getVisitors();
		
		// Istruzioni per inserire il reader nelle liste readerIDs
		// dei nodi source e target.
		for(int i = 0; i < tokens.size(); i++) {
			
			// Nodo source
			if(tokens.get(i).contains("id=")) {
				String idBadgeSourceNode = tokens.get(i).replace("id=", "");
				for(int j = 0; j < prevVisitors.size(); j++) {
					if(idBadgeSourceNode.equals(String.valueOf(prevVisitors.get(j).getBadgeid()))) {
						boolean readerInTag = false;
						for(int k = 0; k < prevVisitors.get(j).getReaderIDs().size(); k++) {
							if(prevVisitors.get(j).getReaderIDs().get(k).equals(idReader)) {
								readerInTag = true;
								break;
							}
						}
						
						if(!readerInTag) {
							prevVisitors.get(j).getReaderIDs().add(idReader);
						}
					}
				}
			}
			
			else
			
			// Nodo target
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\d+.*")) {
				String idBadgeTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", "");
				for(int j = 0; j < prevVisitors.size(); j++) {
					if(idBadgeTargetNode.equals(String.valueOf(prevVisitors.get(j).getBadgeid()))) {
						boolean readerInTag = false;
						for(int k = 0; k < prevVisitors.get(j).getReaderIDs().size(); k++) {
							if(prevVisitors.get(j).getReaderIDs().get(k).equals(idReader)) {
								readerInTag = true;
								break;
							}
						}
						
						if(!readerInTag) {
							prevVisitors.get(j).getReaderIDs().add(idReader);
						}
					}
				}
			}
		}
	}


	private void newTimestampObject(ArrayList<String> tokens, String currentTimestamp) {
		Integer idPersonSourceNode = -1;
		Integer idPersonTargetNode = -1;
		
		TimestampObject timestampObject = new TimestampObject();
		
		timestampObject.setId(indexObjects);
		
		timestampObject.setTimestamp(currentTimestamp);
		
		ArrayList<Visitor> visitors = new ArrayList<Visitor>();
		timestampObject.setVisitors(visitors);
		
		ArrayList<Seq> seqs = new ArrayList<Seq>();
		timestampObject.setSeqNumbers(seqs);
		
		ArrayList<Reader> readers = new ArrayList<Reader>();
		timestampObject.setReaders(readers);
		
		ArrayList<Edge> edges = new ArrayList<Edge>();
		timestampObject.setEdges(edges);
		
		for(int i = 0; i < tokens.size(); i++) {

			/* 
			 * RILEVAMENTI
			 */
			
			if(tokens.get(i).contains("id=")) {
				String idBadgeSourceNode = tokens.get(i).replace("id=", "");
				int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeSourceNode), currentTimestamp);
				if(indexNodeDB >= 0) {
					ArrayList<String> readerIDs = new ArrayList<String>();
					readerIDs.add(tokens.get(2).replace("ip=", ""));
					Visitor visitor = new Visitor(usersDB.get(indexNodeDB));
					visitor.setReaderIDs(readerIDs);
					idPersonSourceNode = visitor.getPersonid();
					visitors.add(visitor);
				} else {
					logger.error("No match for the tag " + idBadgeSourceNode + " for TS " + currentTimestamp);
				}
				
			}
			
			else
			
			if(tokens.get(i).contains("ip=")) {
				String idReader = tokens.get(i).replace("ip=", "");
				//TODO implementare la ricerca per la stanza ed il piano
				readers.add(new Reader(idReader,"compareRoom"));
			}
			
			else					
			
			if(tokens.get(i).contains("seq=")) {
				String seqNumber = tokens.get(i).replace("seq=", "");
				Integer badgeSourceNode = Integer.parseInt(tokens.get(3).replace("id=", ""));
				
				seqs.add(new Seq(seqNumber, badgeSourceNode));
			}
			
			
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\d+.*")) {
				String idBadgeTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\d+\\x29", "");
				int indexNodeDB = searchVisitorInDB(Integer.parseInt(idBadgeTargetNode), currentTimestamp);
				if(indexNodeDB >= 0) {
					ArrayList<String> readerIDs = new ArrayList<String>();
					readerIDs.add(tokens.get(2).replace("ip=", ""));
					Visitor visitor = new Visitor(usersDB.get(indexNodeDB));
					visitor.setReaderIDs(readerIDs);
					idPersonTargetNode = visitor.getPersonid();
					visitors.add(visitor);
				} else {
					logger.error("No match for the tag " + idBadgeTargetNode + " for TS " + currentTimestamp);
				}
				
				// Creazione arco se entrambi i nodi sono stati trovati nel DB
				if(!idPersonSourceNode.equals(-1) && !idPersonTargetNode.equals(-1)) {
					ArrayList<Integer> edgeAttr = new ArrayList<Integer>();
					edgeAttr.add(idPersonSourceNode);
					edgeAttr.add(idPersonTargetNode);
					
					//TODO implementare l'inserimento della potenza
					int power = 0;
					
					edges.add(new Edge(edgeAttr, power));
					
				} else {
					logger.error("No edge to build: idPersonSourceNode or idPersonTargetNode wasn't found in DB.");
				}
			}
		}
		
		tsObjectList.add(timestampObject);
		indexObjects++;
	}
	
	
	private int searchVisitorInDB(Integer id, String currentTimestamp) {
		int found = -1;
		Integer timestamp = Integer.parseInt(currentTimestamp);
		
		for(int i = 0; i < usersDB.size(); i++) {
			Visitor visitor = usersDB.get(i);
			
			if(id.equals(Integer.valueOf(visitor.getBadgeid())) && timestamp >= visitor.getStartdate()) {
				if(visitor.getEnddate() == -1 || timestamp <= visitor.getEnddate()) {
					found = i;
					break;
				}
			}
		}
		
		return found;
	}
	
	
	private ArrayList<Visitor> readDB() {
		ArrayList<Visitor> visitorDB = new ArrayList<Visitor>();
		
		DBObject db = new DBObject(DBConstants.DB_TYPE, DBConstants.DB_SERVER_HOSTNAME, DBConstants.DB_SERVER_PORT, DBConstants.MACRO_DB_NAME, DBConstants.MACRO_DB_USERNAME, DBConstants.MACRO_DB_PASSWORD);
		Connection connection = null;
		try {
			
			connection = db.getConnection();			
			logger.info("Connected to database " + DBConstants.MACRO_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
			
			Statement statement = connection.createStatement();
			String querySelect = "SELECT * FROM " + DBConstants.MACRO_USER_TABLE;
			ResultSet rsSelect = statement.executeQuery(querySelect);
			
			while(rsSelect.next()) {
				Visitor visitor = new Visitor(
						rsSelect.getInt("personid"),
						rsSelect.getInt("badgeid"),
						rsSelect.getString("sex"),
						rsSelect.getInt("age"),
						rsSelect.getString("grade"),
						rsSelect.getString("occupation"),
						rsSelect.getString("nationality"),
						rsSelect.getBoolean("guide"),
						rsSelect.getInt("groupid"),
						rsSelect.getInt("startdate"),
						rsSelect.getInt("enddate"),
						null // ReaderIDs
						);
				
				visitorDB.add(visitor);
			}
			
			statement.close();
			Collections.sort(visitorDB);
			
			for(int i = 0; i < visitorDB.size(); i++) {
				logger.debug(visitorDB.get(i).toString());
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		} finally {
			if (connection != null) {
		    	  try {
					connection.close();
					logger.info("Disconnected to database " + DBConstants.MACRO_DB_NAME + " @ " + DBConstants.DB_SERVER_HOSTNAME);
				} catch (SQLException e) {
					logger.error(e.getMessage());
					//e.printStackTrace();
				}
			}
	    }
		
		return visitorDB;
	}


	public void readLog(Integer startTS, Integer endTS) throws IOException {
		logger.info("Reading...");
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fis));
			
			String currentLine;
			while((currentLine=br.readLine()) != null) {
				String s = currentLine.trim();
				if(!s.equalsIgnoreCase("")) {
					logger.debug(s);
					packetParsing(s, startTS, endTS);
				}
			}
		} finally {
			if(br != null) {
				br.close();
			}
		}
	}

		
	/*
	 * INNER CLASS
	 */
	protected class TimestampObject {
		private int id;
		private String timestamp;
		private ArrayList<Visitor> visitors;
		private ArrayList<Seq> seqNumbers;
		private ArrayList<Reader> readers;
		private ArrayList<Edge> edges;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}
		
		public ArrayList<Visitor> getVisitors() {
			return visitors;
		}

		public void setVisitors(ArrayList<Visitor> visitors) {
			this.visitors = visitors;
		}

		public ArrayList<Seq> getSeqNumbers() {
			return seqNumbers;
		}

		public void setSeqNumbers(ArrayList<Seq> seqNumbers) {
			this.seqNumbers = seqNumbers;
		}

		public ArrayList<Reader> getReaders() {
			return readers;
		}

		public void setReaders(ArrayList<Reader> readers) {
			this.readers = readers;
		}
		
		public ArrayList<Edge> getEdges() {
			return edges;
		}

		public void setEdges(ArrayList<Edge> edges) {
			this.edges = edges;
		}
	}
	
	
	protected class Visitor implements Comparable<Visitor> {
		private int personid;
		private int badgeid;
		private String sex;
		private int age;
		private String grade;
		private String occupation;
		private String nationality;
		private boolean guide;
		private int groupid;
		private Integer startdate;
		private Integer enddate;
		private ArrayList<String> readerIDs;
		
		public Visitor(int personid, int badgeid, String sex, int age, String grade, String occupation,
				String nationality, boolean guide, int groupid, Integer startdate, Integer enddate, ArrayList<String> readerIDs) {
			this.personid = personid;
			this.badgeid = badgeid;
			this.sex = sex;
			this.age = age;
			this.grade = grade;
			this.occupation = occupation;
			this.nationality = nationality;
			this.guide = guide;
			this.groupid = groupid;
			this.startdate = startdate;
			this.enddate = enddate;
			this.readerIDs = readerIDs;
		}
		
		/** Copy Constructor */
		public Visitor(Visitor v) {
			this.personid = v.personid;
			this.badgeid = v.badgeid;
			this.sex = v.sex;
			this.age = v.age;
			this.grade = v.grade;
			this.occupation = v.occupation;
			this.nationality = v.nationality;
			this.guide = v.guide;
			this.groupid = v.groupid;
			this.startdate = v.startdate;
			this.enddate = v.enddate;
			this.readerIDs = v.readerIDs;
		}
		
		public int getPersonid() {
			return personid;
		}
		
		public int getBadgeid() {
			return badgeid;
		}
		
		public String getSex() {
			return sex;
		}

		public int getAge() {
			return age;
		}

		public String getGrade() {
			return grade;
		}

		public String getOccupation() {
			return occupation;
		}

		public String getNationality() {
			return nationality;
		}

		public boolean isGuide() {
			return guide;
		}

		public int getGroupid() {
			return groupid;
		}

		public Integer getStartdate() {
			return startdate;
		}

		public Integer getEnddate() {
			return enddate;
		}

		public ArrayList<String> getReaderIDs() {
			return readerIDs;
		}


		public void setReaderIDs(ArrayList<String> readerIDs) {
			this.readerIDs = readerIDs;
		}

		public int compareTo(Visitor t) {
			return Integer.valueOf(personid).compareTo(t.personid);
		}

		@Override
		public String toString() {
			return "Visitor [personid=" + personid + ", badgeid=" + badgeid
					+ ", sex=" + sex + ", age=" + age + ", grade=" + grade
					+ ", occupation=" + occupation + ", nationality="
					+ nationality + ", guide=" + guide + ", groupid=" + groupid
					+ ", startdate=" + startdate + ", enddate=" + enddate
					+ ", readerIDs=" + readerIDs + "]";
		}
	}
	
	
	private class Seq {
		private String seqNumber;
		private int tagID;
		
		public Seq(String seqNumber, int tagID) {
			this.seqNumber = seqNumber;
			this.tagID = tagID;
		}
		
		public String getSeqNumber() {
			return seqNumber;
		}
		
		public int getTagID() {
			return tagID;
		}
	}
	
	
	protected class Reader {
		private String readerID;
		private String room;
		
		public Reader(String readerID, String room) {
			this.readerID = readerID;
		}
		
		public String getReaderID() {
			return readerID;
		}

		public String getRoom() {
			return room;
		}
	}
	
	
	protected class Edge {
		private ArrayList<Integer> source_target;
		private int power;
		
		public Edge(ArrayList<Integer> source_target, int power) {
			this.source_target = source_target;
			this.power = power;
		}

		public ArrayList<Integer> getSourceTarget() {
			return source_target;
		}

		public int getPower() {
			return power;
		}
	}



}
