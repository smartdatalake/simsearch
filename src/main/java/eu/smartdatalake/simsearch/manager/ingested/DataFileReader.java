package eu.smartdatalake.simsearch.manager.ingested;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Custom reader that can access either local files or remote ones available from HTTP/FTP servers (without credentials).
 * Currently, only working with CSV files.
 */
public class DataFileReader {

	BufferedReader br;           // Instantiates a buffered reader that will consume the contents of the file
	
	/**
	 * Constructor
	 * @param fileName  The path to a local file or its URL on a remote HTTP/FTP server.
	 */
	public DataFileReader(String fileName) {
		
		try {
			if (fileName.startsWith("http") || fileName.startsWith("ftp")) {  // Remote file
				// Open a connection to access the file
				URL url = new URL(fileName);
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				if (connection.getResponseCode() == 200) {   // Connection is open
					InputStreamReader streamReader = new InputStreamReader(connection.getInputStream());
					br = new BufferedReader(streamReader);
				} 
			}
			else {  		// Local file
				br = new BufferedReader(new FileReader(fileName));
			}
//			System.out.println("Access to file: " + fileName + " established.");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * Reads the next line from the opened file.
	 * @return  A string containing the next line of the file.
	 * @throws IOException Thrown if the file is missing. 
	 */
	public String readLine() throws IOException {
		
		return br.readLine();
	}

	
	/**
	 * Closes the file (and the possible connection to the remote server).
	 * @throws IOException  Thrown if the file is missing. 
	 */
	public void close() throws IOException {
		
		br.close();
	}
}
