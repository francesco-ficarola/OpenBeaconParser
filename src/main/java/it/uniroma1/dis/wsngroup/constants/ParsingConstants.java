package it.uniroma1.dis.wsngroup.constants;

/**
 * @author Francesco Ficarola
 *
 */

public class ParsingConstants {
	public static final int TYPE_MESSAGE_INDEX = 0;
	public static final int TIMESTAMP_INDEX = 1;
	public static final int READER_INDEX = 2;
	public static final int SOURCE_INDEX = 3;
	public static final int CUSTOM_INDEX = 4;
	public static final int SEQ_INDEX = 5;
	public static final String TIMESTAMP_PREFIX = "t=";
	public static final String READER_PREFIX = "ip=";
	public static final String SOURCE_PREFIX = "id=";
	public static final String CUSTOM_PREFIX = "boot_count=";
	public static final String SEQ_PREFIX = "seq=";
	
	public static final int MAX_CONTACT_POWER = 1;
	
	public static final int ADJACENCY_LISTS_REPRESENTATION = 1;
	public static final int ADJACENCY_MATRIX_REPRESENTATION = 2;
	public static final int GEXF_REPRESENTATION = 3;
	public static final int JSON_REPRESENTATION = 4;
	public static final int NETLOGO_REPRESENTATION = 5;
	public static final int DNF_REPRESENTATION = 6;
	public static final int STATISTICS = 7;
	public static final String TAB_SEPARATOR = "\t";
	public static final String SEMICOLON_SEPARATOR = ";";
	public static final String COMMA_SEPARATOR = ",";
	public static final String EMPTY_SEPARATOR = "";
	public static final String TXT_FILE_TYPE = ".txt";
	public static final String CSV_FILE_TYPE = ".csv";
	public static final String GEXF_FILE_TYPE = ".gexf";
	public static final String JSON_FILE_TYPE = ".json";
	public static final String DNF_FILE_TYPE = ".dnf";
	public static final int MAXIMUM_FRACTION_DIGITS = 5;
	public static final boolean CREATE_VIRTUAL_LINKS = true;
	public static final int JSON_SLEEP_INTERVAL = 1000;
	public static final int MILLI = 1000;
	public static final int REAL_TIME_REFRESH_INTERVAL = 1000;
	public static final int DEFAULT_EDGE_MODE = 1;
	public static final int CONTINUOUS_EDGE_MODE = 2;
	public static final int INTERVAL_EDGE_MODE = 3;
	public static final Integer EFFECTIVE_EDGE_MODE = 4;
}
