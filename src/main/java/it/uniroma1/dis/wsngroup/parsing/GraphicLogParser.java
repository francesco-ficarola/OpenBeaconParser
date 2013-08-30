package it.uniroma1.dis.wsngroup.parsing;

import it.uniroma1.dis.wsngroup.constants.ParsingConstants;
import it.uniroma1.dis.wsngroup.core.DateTime;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.AdjacencyListsModule;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.AdjacencyMatrixModule;
import it.uniroma1.dis.wsngroup.parsing.modules.aggregate.DynamicNetworkModule;
import it.uniroma1.dis.wsngroup.parsing.modules.dis.GexfModuleAggregateDIS;
import it.uniroma1.dis.wsngroup.parsing.modules.dis.JsonModuleDIS;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.GexfModuleDynamic;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.JsonModuleWithoutDB;
import it.uniroma1.dis.wsngroup.parsing.modules.dynamics.NetLogoModule;
import it.uniroma1.dis.wsngroup.parsing.representation.GraphRepresentation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.log4j.Logger;

import javax.swing.JOptionPane;

/**
 * @author Lorenzo Bergamini
 *
 */

@Deprecated
public class GraphicLogParser {
	
    private static Logger logger = Logger.getLogger(GraphicLogParser.class);
    private static String separator;
    private static String fileType;
    private static int representationType;
    private static FileInputStream fis;
    private static File fileInput;
    private static String[] dateTime;
    private static String listGraphOut;
    private static Integer startTS;
    private static Integer endTS;
    private static boolean initReaders;

    public GraphicLogParser() {
        separator = ParsingConstants.TAB_SEPARATOR;
        fileType = ParsingConstants.TXT_FILE_TYPE;
        representationType = ParsingConstants.ADJACENCY_LISTS_REPRESENTATION;
        dateTime = new DateTime().getDateTime();
        listGraphOut = "graph" + "_" + dateTime[0] + "_" + dateTime[1];
        startTS = 0;
        endTS = 2147483647;
        initReaders = false;
    }

    public static void initParsing(String[] values, String nomeFile, int startTimestamp, int endTimestamp) {


        new GraphicLogParser();
        GraphRepresentation graph = null;
        fileInput = new File(nomeFile);
        startTS = startTimestamp;
        endTS = endTimestamp;

        if (fileInput.exists()) {
            try {
                fis = new FileInputStream(fileInput);
            } catch (FileNotFoundException ex) {
                java.util.logging.Logger.getLogger(GraphicLogParser.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            logger.error(fileInput.getAbsolutePath() + "- No such file or directory");
        }


        if (values[1].equals("Adjacency List")) {
            representationType = ParsingConstants.ADJACENCY_LISTS_REPRESENTATION;
        } else if (values[1].equals("Adjacency Matrix")) {
            representationType = ParsingConstants.ADJACENCY_MATRIX_REPRESENTATION;
        } else if (values[1].equals("Netlogo")) {
            separator = ParsingConstants.EMPTY_SEPARATOR;
            representationType = ParsingConstants.NETLOGO_REPRESENTATION;
            fileType = ParsingConstants.TXT_FILE_TYPE;
        } else if (values[1].equals("Dynamic Network Format")) {
            separator = ParsingConstants.EMPTY_SEPARATOR;
            representationType = ParsingConstants.DNF_REPRESENTATION;
            fileType = ParsingConstants.DNF_FILE_TYPE;
        } else if (values[1].equals("Gexf")) {
            separator = ParsingConstants.EMPTY_SEPARATOR;
            representationType = ParsingConstants.GEXF_REPRESENTATION;
            fileType = ParsingConstants.GEXF_FILE_TYPE;
        } else if (values[1].equals("Json")) {
            separator = ParsingConstants.EMPTY_SEPARATOR;
            representationType = ParsingConstants.JSON_REPRESENTATION;
            fileType = ParsingConstants.JSON_FILE_TYPE;
        }

        if (representationType == ParsingConstants.ADJACENCY_LISTS_REPRESENTATION) {
            graph = new AdjacencyListsModule(fileInput, fis);
            logger.info("Adjacency Lists Representation initialized.");
        } else if (representationType == ParsingConstants.ADJACENCY_MATRIX_REPRESENTATION) {
            graph = new AdjacencyMatrixModule(fileInput, fis);
            logger.info("Adjacency Matrix Representation initialized.");
        } else if (representationType == ParsingConstants.GEXF_REPRESENTATION) {

            if (values[3].equals("Dynamic")) {
                if (values[2].equals("YES")) {
                    graph = new GexfModuleDynamic(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS);
                    listGraphOut = listGraphOut + "_virtual_links";
                } else {
                    graph = new GexfModuleDynamic(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS);
                }
                logger.info("Dynamic GEXF Representation initialized.");
            } else if (values[3].equals("Aggregate")) {
                graph = new GexfModuleAggregateDIS(fileInput, fis);
                logger.info("Aggregate GEXF Representation initialized.");
            }
        } else if (representationType == ParsingConstants.JSON_REPRESENTATION) {
            if (values[2].equals("YES") && (values[3].equals("UseDB"))) {
                graph = new JsonModuleDIS(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS);
                listGraphOut = listGraphOut + "_virtual_links";
            } else if (values[2].equals("YES") && (values[3].equals("NotUseDB"))) {
                graph = new JsonModuleWithoutDB(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS);
                listGraphOut = listGraphOut + "_virtual_links";
            } else if (values[2].equals("NO") && (values[3].equals("UseDB"))) {
                graph = new JsonModuleDIS(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS);
            } else if (values[2].equals("NO") && (values[3].equals("NotUseDB"))) {
                graph = new JsonModuleWithoutDB(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS);
            }

            logger.info("JSON Representation initialized.");
        } else if (representationType == ParsingConstants.NETLOGO_REPRESENTATION) {
            if (values[2].equals("YES")) {
                graph = new NetLogoModule(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS, initReaders);
                listGraphOut = listGraphOut + "_virtual_links";
            } else {
                graph = new NetLogoModule(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS, initReaders);
            }
            logger.info("NetLogo Representation initialized.");
        } else if (representationType == ParsingConstants.DNF_REPRESENTATION) {
            if (values[2].equals("YES")) {
                graph = new DynamicNetworkModule(fileInput, fis, ParsingConstants.CREATE_VIRTUAL_LINKS);
                listGraphOut = listGraphOut + "_virtual_links";
            } else {
                graph = new DynamicNetworkModule(fileInput, fis, !ParsingConstants.CREATE_VIRTUAL_LINKS);
            }

            logger.info("DNF Representation initialized.");
        }

        // Total file name
        listGraphOut = listGraphOut + fileType;

        try {
            graph.readLog(startTS, endTS);
            graph.writeFile(listGraphOut, separator);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        logger.info("Done. Good job!");

        if ((JOptionPane.showConfirmDialog(null, "Operation Completed! Do you want to exit now?", "Confirm", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION)) {
            System.exit(0);
        }
        else ;
    }
}
