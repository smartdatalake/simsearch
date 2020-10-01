package eu.smartdatalake.simsearch;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Auxiliary class that keeps a log of executed operations.
 */
public class Logger {

	String logFile;
	PrintStream logStream = null;
	SimpleDateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");         
	
	/**
	 * Constructor		
	 * @param logFile  Path to the file that logs messages, notifications, and statistics.
	 * @param append  True, if notifications should be appended to existing file; otherwise, False.
	 */
	public Logger(String logFile, boolean append) {
		
		try {
			this.logStream = new PrintStream(new FileOutputStream(logFile,append));
			this.logFile = logFile;  // Keep a reference to the logfile path
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		// Time notifications in the log are timestamped in GMT time zone
		gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * Writes the specified message to the log with a timestamp.
	 * @param message  String to be written in the log.
	 */
	public void writeln(String message) {
		
		if (this.logStream != null)
			this.logStream.println(gmtDateFormat.format(new java.util.Date()) + " GMT " + message);
	}
	
}
