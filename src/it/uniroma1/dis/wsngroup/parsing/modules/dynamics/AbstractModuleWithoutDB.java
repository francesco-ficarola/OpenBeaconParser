package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;

import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;


public abstract class AbstractModuleWithoutDB implements GraphRepresentation {
	
	/**
	 * @author Francesco Ficarola
	 *
	 */

	protected Logger logger = Logger.getLogger(this.getClass());
	
	protected File fileInput;
	private FileInputStream fis;
	private boolean createVL;
	protected List<TimestampObject> tsObjectList;
	private int indexObjects;
	private String previousTimestamp;
	
	public AbstractModuleWithoutDB(File fileInput, FileInputStream fis, boolean createVL) {
		this.fileInput = fileInput;
		this.fis = fis;
		this.createVL = createVL;
		tsObjectList = new LinkedList<TimestampObject>();
		indexObjects = 0;
		previousTimestamp = "";
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
			
			// Gestisci il pacchetto corrente (tags e contatti REALI)
			managePacket(tokens);
			
			// Crea contatti VIRTUALI per rendere il grafo meno sparso.
			// Metodo necessario per le sperimentazioni su NetLogo.
			if(createVL) {
				createVirtualEdges();
			}
		}
	}


	private void createVirtualEdges() {
		TimestampObject lastTimestampObject = tsObjectList.get(indexObjects-1);
		if (lastTimestampObject.getTags().size() > 1) {
			ArrayList<Tag> tags = lastTimestampObject.getTags();
			for(int i = 0; i < tags.size(); i++) {
				Tag sourceTag = tags.get(i);
				for(int j = 0; j < tags.size(); j++) {
					if(sourceTag.getTagID() != tags.get(j).getTagID()) {
						Tag destTag = tags.get(j);
						
						// Controllo se l'arco è già presente nell'oggetto
						boolean existingEdge = false;
						String source = sourceTag.getTagID();
						String target = destTag.getTagID();
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
								ArrayList<String> edgeAttr = new ArrayList<String>();
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


	private void managePacket(ArrayList<String> tokens) {
		// Se il timestamp corrente è diverso dal precedente si crea
		// un nuovo oggetto TimestampObject per memorizzare tutto il necessario
		String currentTimestamp = tokens.get(1).replace("t=", "");
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
				String sourceTag = tokens.get(3).replace("id=", "");
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
				newTimestampObject(tokens);
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
					String sourceTag = tokens.get(3).replace("id=", "");
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
					managePrevObject(tokens);
				} else {
					// Caso in cui non è ancora presente un oggetto con
					// il timestamp corrente, di conseguenza lo creo.
					newTimestampObject(tokens);
				}
			}
		}
		
		// Memorizzazione del timestamp corrente in previousTimestamp
		// in modo da poter comparare questo valore con il successivo
		previousTimestamp = tokens.get(1).replace("t=", "");
	}


	private void managePrevObject(ArrayList<String> tokens) {
						
		String idSourceNode = "";
		String idTargetNode = "";
		
		for(int i = 0; i < tokens.size(); i++) {
			
			/* 
			 * TOKEN "id="
			 */
			if(tokens.get(i).contains("id=")) {
				idSourceNode = tokens.get(i).replace("id=", "");
					
				// Controllo se il tag è già presente nell'oggetto
				int existing = -1;
				String idCurrentNode = idSourceNode;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getTags().size(); k++) {
					if(idCurrentNode.equals(tsObjectList.get(indexObjects-1).getTags().get(k).getTagID())) {
						existing = k;
						break;
					}
				}
				
				// Se non esiste, allora lo aggiungo
				if(existing == -1) {
					ArrayList<String> readerIDs = new ArrayList<String>();
					readerIDs.add(tokens.get(2).replace("ip=", ""));
					tsObjectList.get(indexObjects-1).getTags().
								add(new Tag(idCurrentNode, readerIDs));
				} else {
					// Se già esiste devo aggiornare solo la lista dei readerIDs se il corrente non è già presente
					String readerID = tokens.get(2).replace("ip=", "");
					int existingReader = -1;
					for(int z = 0; z < tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().size(); z++) {
						if(readerID.equals(tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().get(z))) {
							existingReader = z;
							break;
						}
					}
					
					if(existingReader == -1) {
						tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().add(tokens.get(2).replace("ip=", ""));
					}
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
					tsObjectList.get(indexObjects-1).getReaders().add(new Reader(idReader));
				}
			}
			
			else

			/*
			 * TOKEN "seq=*"
			 */
			if(tokens.get(i).contains("seq=")) {
				String seqNumber = tokens.get(i).replace("seq=", "");
				String sourceTag = tokens.get(3).replace("id=", "");
				
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
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\w+.*")) {
				idTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\w+\\x29", "");
				
				// Controllo se il tag è già presente nell'oggetto
				int existing = -1;
				String idCurrentNode = idTargetNode;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getTags().size(); k++) {
					if(idCurrentNode.equals(tsObjectList.get(indexObjects-1).getTags().get(k).getTagID())) {
						existing = k;
						break;
					}
				}
				
				// Se non esiste, allora lo aggiungo
				if(existing == -1) {
					ArrayList<String> readerIDs = new ArrayList<String>();
					readerIDs.add(tokens.get(2).replace("ip=", ""));
					tsObjectList.get(indexObjects-1).getTags().
								add(new Tag(idCurrentNode,readerIDs));
				} else {
					// Se già esiste devo aggiornare solo la lista dei readerIDs se il corrente non è già presente
					String readerID = tokens.get(2).replace("ip=", "");
					int existingReader = -1;
					for(int z = 0; z < tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().size(); z++) {
						if(readerID.equals(tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().get(z))) {
							existingReader = z;
							break;
						}
					}
					
					if(existingReader == -1) {
						tsObjectList.get(indexObjects-1).getTags().get(existing).getReaderIDs().add(tokens.get(2).replace("ip=", ""));
					}
				}
				
				// Controllo se l'arco è già presente nell'oggetto
				boolean existingEdge = false;
				String source = idSourceNode;
				String target = idTargetNode;
				for(int k = 0; k < tsObjectList.get(indexObjects-1).getEdges().size(); k++) {
					if((source.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(0)) &&
						target.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(1)))
						||
						(target.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(0)) &&
						source.equals(tsObjectList.get(indexObjects-1).getEdges().get(k).getSourceTarget().get(1)))) {
						existingEdge = true;
						break;
					}
				}
				
				// Se non esiste, allora lo aggiungo
				if(!existingEdge) {
					ArrayList<String> edgeAttr = new ArrayList<String>();
					edgeAttr.add(idSourceNode);
					edgeAttr.add(idTargetNode);
					
					//TODO implementare l'inserimento della potenza
					int power = 0;
					
					tsObjectList.get(indexObjects-1).getEdges().
									add(new Edge(edgeAttr, power));
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
			prevComparingTimestampObject.getReaders().add(new Reader(idReader));						
		}
		
		// Provvedo ad aggiungere il reader nei tag con cui ha comunicato
		// qualora già non lo posseggono.
		
		// Lista dei visitatori precedenti
		ArrayList<Tag> prevVisitors = prevComparingTimestampObject.getTags();
		
		// Istruzioni per inserire il reader nelle liste readerIDs
		// dei nodi source e target.
		for(int i = 0; i < tokens.size(); i++) {
			
			// Nodo source
			if(tokens.get(i).contains("id=")) {
				String idBadgeSourceNode = tokens.get(i).replace("id=", "");
				for(int j = 0; j < prevVisitors.size(); j++) {
					if(idBadgeSourceNode.equals(String.valueOf(prevVisitors.get(j).getTagID()))) {
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
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\w+.*")) {
				String idBadgeTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\w+\\x29", "");
				for(int j = 0; j < prevVisitors.size(); j++) {
					if(idBadgeTargetNode.equals(String.valueOf(prevVisitors.get(j).getTagID()))) {
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


	private void newTimestampObject(ArrayList<String> tokens) {
		String idSourceNode = "";
		String idTargetNode = "";
		
		TimestampObject timestampObject = new TimestampObject();
		
		timestampObject.setId(indexObjects);
		
		timestampObject.setTimestamp(tokens.get(1).replace("t=", ""));
		
		ArrayList<Tag> tags = new ArrayList<Tag>();
		timestampObject.setTags(tags);
		
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
				idSourceNode = tokens.get(i).replace("id=", "");
				ArrayList<String> readerIDs = new ArrayList<String>();
				readerIDs.add(tokens.get(2).replace("ip=", ""));
				tags.add(new Tag(idSourceNode, readerIDs));
				
			}
			
			else
			
			if(tokens.get(i).contains("ip=")) {
				String idReader = tokens.get(i).replace("ip=", "");
				readers.add(new Reader(idReader));
			}
			
			else					
			
			if(tokens.get(i).contains("seq=")) {
				String seqNumber = tokens.get(i).replace("seq=", "");
				String sourceTag = tokens.get(3).replace("id=", "");
				
				seqs.add(new Seq(seqNumber, sourceTag));
			}
			
			
			if(tokens.get(0).equals("C") && tokens.get(i).matches("\\x5b\\w+.*")) {
				idTargetNode = tokens.get(i).replaceAll("\\x5b|\\x28\\w+\\x29", "");
				ArrayList<String> readerIDs = new ArrayList<String>();
				readerIDs.add(tokens.get(2).replace("ip=", ""));
				tags.add(new Tag(idTargetNode, readerIDs));
			
				ArrayList<String> edgeAttr = new ArrayList<String>();
				edgeAttr.add(idSourceNode);
				edgeAttr.add(idTargetNode);
				
				//TODO implementare l'inserimento della potenza
				int power = 0;
				
				edges.add(new Edge(edgeAttr, power));
			}
		}
		
		tsObjectList.add(timestampObject);
		indexObjects++;
	}
	

	@Override
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
		private ArrayList<Tag> tags;
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
		
		public ArrayList<Tag> getTags() {
			return tags;
		}

		public void setTags(ArrayList<Tag> tags) {
			this.tags = tags;
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
	
	
	protected class Tag implements Comparable<Tag> {
		private String tagID;
		private ArrayList<String> readerIDs;
		
		public Tag(String tagID, ArrayList<String> readerIDs) {
			this.tagID = tagID;
			this.readerIDs = readerIDs;
		}
		
		public Tag(String tagID) {
			this.tagID = tagID;
		}
		
		public String getTagID() {
			return tagID;
		}
		
		public ArrayList<String> getReaderIDs() {
			return readerIDs;
		}


		@Override
		public int compareTo(Tag t) {
			return tagID.compareTo(t.tagID);
		}
		
		@Override
		public String toString() {
			return "Tag [tagID=" + tagID + "]";
		}
	}
	
	
	private class Seq {
		private String seqNumber;
		private String tagID;
		
		public Seq(String seqNumber, String tagID) {
			this.seqNumber = seqNumber;
			this.tagID = tagID;
		}
		
		public String getSeqNumber() {
			return seqNumber;
		}
		
		public String getTagID() {
			return tagID;
		}
	}
	
	
	protected class Reader {
		private String readerID;
		
		public Reader(String readerID) {
			this.readerID = readerID;
		}
		
		public String getReaderID() {
			return readerID;
		}
	}
	
	
	protected class Edge {
		private ArrayList<String> source_target;
		private int power;
		
		public Edge(ArrayList<String> source_target, int power) {
			this.source_target = source_target;
			this.power = power;
		}

		public ArrayList<String> getSourceTarget() {
			return source_target;
		}

		public int getPower() {
			return power;
		}
	}

}

