package it.uniroma1.dis.wsngroup.core.realtime;

/**
 * Provides listener notification methods when a tailed log file is updated
 */
public interface RealTimeLogFileTailerListener
{
  /**
   * A new line has been added to the tailed log file
   * 
   * @param line   The new line that has been added to the tailed log file
   */
  public void newLogFileLine( String line );
}