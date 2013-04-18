package it.uniroma1.dis.wsngroup.parsing;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.core.realtime.RealTimeLogFileTailer;
import it.uniroma1.dis.wsngroup.core.realtime.RealTimeLogFileTailerListener;
import it.uniroma1.dis.wsngroup.parsing.modules.realtime.RealTimeModule;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;


public class GraphicRealTimeParser implements RealTimeLogFileTailerListener {

    /**
     * @author Francesco Ficarola
     *
     */
    private static Logger logger = Logger.getLogger(GraphicRealTimeParser.class);
    private static FileInputStream fis;
    private static File fileInput;
    private static Integer startTS;
    private static Integer endTS;
    private static GraphRepresentation graph;
    private static boolean createVL;
    private RealTimeLogFileTailer tailer;

    public GraphicRealTimeParser() {
        startTS = 0;
        endTS = 2147483647;
        graph = null;
        createVL = false;
        tailer = new RealTimeLogFileTailer(fileInput, ParsingConstants.REAL_TIME_REFRESH_INTERVAL, false);
        tailer.addRealTimeLogFileTailerListener(this);
        tailer.start();
    }

    public static boolean initRealTimeParsing(String[] values, String nomeFile) throws IOException {
        boolean missingfile = true;

        fileInput = new File(nomeFile);
        

        if (fileInput.exists()) {
            missingfile = false;
            fis = new FileInputStream(fileInput);
            new GraphicRealTimeParser();

            if (values[2].equals("YES")) {
                createVL = true;
            }

            graph = new RealTimeModule(fileInput, fis, createVL);

        } else {
           logger.error(fileInput.getAbsolutePath() + "- No such file or directory");
        }

        return missingfile;
    }

    public void newLogFileLine(String line) {
        try {
            graph.readLog(startTS, endTS);
        } catch (IOException e) {
            logger.error(e.getMessage());
//			e.printStackTrace();
        }
    }
}
