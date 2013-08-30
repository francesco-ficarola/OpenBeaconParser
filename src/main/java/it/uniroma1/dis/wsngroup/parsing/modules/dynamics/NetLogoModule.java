package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

/**
 * @author Francesco Ficarola
 *
 */

public class NetLogoModule extends AbstractModuleWithoutDB {
	
	private boolean initReaders;
	
	public NetLogoModule(File fileInput, FileInputStream fis, boolean createVL, boolean initReaders) {
		super(fileInput, fis, createVL);
		this.initReaders = initReaders;
	}

	public void writeFile(String outputFileName, String separator) throws IOException {
		if(tsObjectList.size() > 0) {
			logger.info("Writing...");
			
			File f = new File(fileInput.getParentFile() + "/" + outputFileName);
			FileOutputStream fos = new FileOutputStream(f, true);
			PrintStream ps = new PrintStream(fos);
			
			Iterator<TimestampObject> it = tsObjectList.iterator();
			while(it.hasNext()) {
				TimestampObject curTimestampObject = it.next();
				
				// Open root list
				ps.print("[");
				ps.print(curTimestampObject.getId());
				ps.print(" ");
				ps.print(curTimestampObject.getTimestamp());
				
				// Open nodes list
				ps.print(" [\"nodes\"");
				
				// Readers data
				if(initReaders) {
					if(curTimestampObject.getReaders().size() > 0) {
						ps.print(" ");
						for(int i = 0; i < curTimestampObject.getReaders().size(); i++) {
							String readerId = curTimestampObject.getReaders().get(i).getReaderID();
							readerId = readerId.substring(readerId.length()-4, readerId.length());
							ps.print(Integer.parseInt(readerId, 16));
							if(i < curTimestampObject.getReaders().size() - 1) {
								ps.print(" ");
							}
						}
					}
				}
				
				// Tags data
				if(curTimestampObject.getTags().size() > 0) {
					ps.print(" ");
					for(int i = 0; i < curTimestampObject.getTags().size(); i++) {
						ps.print(curTimestampObject.getTags().get(i).getTagID());
						if(i < curTimestampObject.getTags().size() - 1) {
							ps.print(" ");
						}
					}
				}
				
				// Closing nodes list
				ps.print("]");
				
				// Open edges list
				ps.print(" [\"edges\"");
				
				// Edges node-reader data
				if(initReaders) {
					if(curTimestampObject.getReaders().size() > 0 && curTimestampObject.getTags().size() > 0) {
						ps.print(" ");
						for(int i = 0; i < curTimestampObject.getTags().size(); i++) {
							Tag currentTag = curTimestampObject.getTags().get(i);
							for(int j = 0; j < currentTag.getReaderIDs().size(); j++) {
								String readerId = currentTag.getReaderIDs().get(j);
								readerId = readerId.substring(readerId.length()-4, readerId.length());
								
								ps.print("[");
								ps.print(currentTag.getTagID());
								ps.print(" ");
								ps.print(Integer.parseInt(readerId, 16));
								
								if(j < currentTag.getReaderIDs().size() - 1) {
									ps.print("] ");
								} else {
									ps.print("]");
								}
							}
							if(i < curTimestampObject.getTags().size() - 1) {
								ps.print(" ");
							}
						}
					}
				}
				
				// Edges node-node data
				if(curTimestampObject.getEdges().size() > 0) {
					ps.print(" ");
					for(int i = 0; i < curTimestampObject.getEdges().size(); i++) {
						Edge e = curTimestampObject.getEdges().get(i);
						ps.print("[");
						ps.print(e.getSourceTarget().get(0));
						ps.print(" ");
						ps.print(e.getSourceTarget().get(1));
						
						if(i < curTimestampObject.getEdges().size() - 1) {
							ps.print("] ");
						} else {
							ps.print("]");
						}
					}
				}
				
				// Closing edges list
				ps.print("]");
				
				// Closing root list
				ps.print("]\n");
			}
			
			logger.info("Output file: " + f.getAbsolutePath());
			ps.close();
		}
	}
}
