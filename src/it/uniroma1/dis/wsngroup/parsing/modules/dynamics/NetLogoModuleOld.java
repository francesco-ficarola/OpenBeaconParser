package it.uniroma1.dis.wsngroup.parsing.modules.dynamics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

@Deprecated
public class NetLogoModuleOld extends AbstractModuleWithoutDBold {

	public NetLogoModuleOld(File fileInput, FileInputStream fis, boolean createVL, Integer idRadix) {
		super(fileInput, fis, createVL, idRadix);
	}

	@Override
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
				
				// Open tags list
				ps.print(" [\"tags\"");
				
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
				
				// Closing tags list
				ps.print("]");
				
				// Open edges list
				ps.print(" [\"edges\"");
				
				// Edges data
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
