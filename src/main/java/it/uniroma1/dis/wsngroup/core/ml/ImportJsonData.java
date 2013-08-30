package it.uniroma1.dis.wsngroup.core.ml;

import it.uniroma1.dis.wsngroup.core.ml.Classification.TimestampObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author Francesco Ficarola
 */

public class ImportJsonData {
	
	private Logger logger = Logger.getLogger(this.getClass());

	private FileInputStream fis;
	private Gson gson;
	
	public ImportJsonData(FileInputStream fis) {
		this.fis = fis;
		gson = new Gson();
	}


	@SuppressWarnings("unchecked")
	public List<TimestampObject> readJson() {
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String jsonString;
		List<TimestampObject> tsObjectList = null;
		try {
			jsonString= br.readLine();
			Type collectionType = new TypeToken<List<TimestampObject>>(){}.getType();
			tsObjectList = (List<TimestampObject>) gson.fromJson(jsonString, collectionType);
		} catch (IOException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
		return tsObjectList;
	}

}
