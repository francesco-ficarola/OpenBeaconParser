package it.uniroma1.dis.wsngroup.parsing.representation;

import java.io.IOException;

/**
 * @author Francesco Ficarola
 *
 */

public interface GraphRepresentation {
	public void readLog(Integer startTS, Integer endTS) throws IOException;
	public void writeFile(String outputFileName, String separator) throws IOException;
}
