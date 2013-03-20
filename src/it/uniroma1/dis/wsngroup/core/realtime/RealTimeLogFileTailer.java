package it.uniroma1.dis.wsngroup.core.realtime;

import java.io.*;
import java.util.*;

/**
 * A log file tailer is designed to monitor a log file and send notifications
 * when new lines are added to the log file. This class has a notification
 * strategy similar to a SAX parser: implement the RealTimeLogFileTailerListener interface,
 * create a RealTimeLogFileTailer to tail your log file, add yourself as a listener, and
 * start the RealTimeLogFileTailer. It is your job to interpret the results, build meaningful
 * sets of data, etc. This tailer simply fires notifications containing new log file lines, 
 * one at a time.
 */
public class RealTimeLogFileTailer extends Thread 
{
  /**
   * How frequently to check for file changes; defaults to 5 seconds
   */
  private long sampleInterval = 1000;

  /**
   * The log file to tail
   */
  private File logfile;

  /**
   * Defines whether the log file tailer should include the entire contents
   * of the exising log file or tail from the end of the file when the tailer starts
   */
  private boolean startAtBeginning = true;

  /**
   * Is the tailer currently tailing?
   */
  private boolean tailing = false;

  /**
   * Set of listeners
   */
  private Set<RealTimeLogFileTailerListener> listeners = new HashSet<RealTimeLogFileTailerListener>();

  /**
   * Creates a new log file tailer that tails an existing file and checks the file for
   * updates every 1000ms
   */
  public RealTimeLogFileTailer( File file )
  {
    this.logfile = file;
  }

  /**
   * Creates a new log file tailer
   * 
   * @param file         The file to tail
   * @param sampleInterval    How often to check for updates to the log file (default = 5000ms)
   * @param startAtBeginning   Should the tailer simply tail or should it process the entire
   *               file and continue tailing (true) or simply start tailing from the 
   *               end of the file
   */
  public RealTimeLogFileTailer( File file, long sampleInterval, boolean startAtBeginning )
  {
    this.logfile = file;
    this.sampleInterval = sampleInterval;
  }

  public void addRealTimeLogFileTailerListener( RealTimeLogFileTailerListener l )
  {
    this.listeners.add( l );
  }

  public void removeRealTimeLogFileTailerListener( RealTimeLogFileTailerListener l )
  {
    this.listeners.remove( l );
  }

  protected void fireNewLogFileLine( String line )
  {
    for( Iterator<RealTimeLogFileTailerListener> i=this.listeners.iterator(); i.hasNext(); )
    {
      RealTimeLogFileTailerListener l = ( RealTimeLogFileTailerListener )i.next();
      l.newLogFileLine( line );
    }
  }

  public void stopTailing()
  {
    this.tailing = false;
  }

  public void run()
  {
    // The file pointer keeps track of where we are in the file
    long filePointer = 0;

    // Determine start point
    if( this.startAtBeginning )
    {
      filePointer = 0;
    }
    else
    {
      filePointer = this.logfile.length();
    }

    try
    {
      // Start tailing
      this.tailing = true;
      RandomAccessFile file = new RandomAccessFile( logfile, "r" );
      while( this.tailing )
      {
        try
        {  
          // Compare the length of the file to the file pointer
          long fileLength = this.logfile.length();
          if( fileLength < filePointer ) 
          {
            // Log file must have been rotated or deleted; 
            // reopen the file and reset the file pointer
            file = new RandomAccessFile( logfile, "r" );
            filePointer = 0;
          }

          if( fileLength > filePointer ) 
          {
            // There is data to read
            file.seek( filePointer );
            String line = file.readLine();
            while( line != null )
            {
              this.fireNewLogFileLine( line );
              line = file.readLine();
            }
            filePointer = file.getFilePointer();
          }

          // Sleep for the specified interval
          sleep( this.sampleInterval );
        }
        catch( Exception e )
        {
        }
      }

      // Close the file that we are tailing
      file.close();
    }
    catch( Exception e )
    {
      e.printStackTrace();
    }
  }
}