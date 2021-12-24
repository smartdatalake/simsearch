package eu.smartdatalake.simsearch.engine;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.smartdatalake.simsearch.engine.processor.RankedResult;
import eu.smartdatalake.simsearch.engine.processor.ResultFacet;

/**
 * Auxiliary class to formulate search response in a tabular format for printing in a text file or to the standard output (console).
 */
public class SearchResponseTable {

	String newline;
	
	/**
	 * Constructor
	 */
	public SearchResponseTable() {
		
		newline = System.getProperty("line.separator");   // Use system-dependent line separator
	}


	/**
	 * Presents the search results in a tabular format for printing to a text file or to the standard output (console).
	 * CAUTION! Long attribute values are wrapped into multiple lines.
	 * @param weights  The weights applied to the specified batch of results. Assuming only ONE weight value per attribute.
	 * @param results  Output results as received from the rank aggregation process (for a given combination of weights).
	 * @param responseTime  Time (in seconds) taken to provide the response by the processing engine.
	 * @return  A string that contains all results in tabular format (a header with column names, and one line per row).
	 */
	public String print(Map<String, Double> weights, IResult[] results, double responseTime) {
		
		StringBuilder txtResult = new StringBuilder();
		DecimalFormat df = new DecimalFormat("0.0#####");
		txtResult.append(newline);

		if (results.length == 0) {
			txtResult.append("Query timed out with no results.");
			return txtResult.toString();
		}
		
		// Mandatory rank-related columns to appear in all results
		ArrayList<String> columnNames = new ArrayList<>(Arrays.asList("rank", "score", "id"));
		int maxLength = 40;   // Maximum allowed length per column; line will be wrapped beyond this value
		
		// Length of the various columns
		Map<String, Integer> columnLengths = new HashMap<>();
		columnLengths.put("rank", 4);  		// fixed length for rank
		columnLengths.put("score", 8);  	// fixed length for score  //df.getMaximumIntegerDigits() + df.getMaximumFractionDigits() + 1
		columnLengths.put("id", 2);  		// initial length for entity id
	
		// Initial length for attribute values
		for (ResultFacet col: results[0].getAttributes()) { 
			if (!columnNames.contains(col.getName()))
				columnNames.add(col.getName());  
			// Initial length for each attribute based on its name and weight
			columnLengths.put(col.getName(), col.getName().length() + df.format(weights.get(col.getName())).length() + 2);  
		}

		// Initial length for extra attribute values
		for (String extraColumn: ((RankedResult)results[0]).getExtraAttributes().keySet()) { 
			columnNames.add(extraColumn);
			// Initial length based on the name of each extra attribute
			columnLengths.put(extraColumn, extraColumn.length());  		
		}
		
		// List of 1-dimensional array to hold rows of unformatted results
		List<String[]> listRows = new ArrayList<>();

		// Copy all values as one row into the list
		// Also determine lengths of attribute values
		for (IResult res: results) {
			String[] row = new String[columnLengths.size()];
			row[0] = "" + res.getRank();  			// rank
			row[1] = df.format(res.getScore());		// score
			// entity identifier
			row[2] = res.getId();								
			if (columnLengths.get("id") < res.getId().length()) {
				columnLengths.put("id", res.getId().length());
			}		
			// Copy all attribute values involved in similarity search
			int j = 3;
			for (ResultFacet col : res.getAttributes()) {
				row[j] = getString(col.getValue());
				// Determine length
				if (columnLengths.get(col.getName()) < row[j].length()) {
					columnLengths.put(col.getName(), row[j].length());
				}
				j++;
			}
			
			// Copy any extra attribute values
			for (Map.Entry<String, ?> entry : ((RankedResult)res).getExtraAttributes().entrySet())  {
				row[j] = getString(entry.getValue());
				// Determine length
				if (columnLengths.get(entry.getKey()) < row[j].length()) {
					columnLengths.put(entry.getKey(), row[j].length());
				}
				j++;
			}
				
			listRows.add(row);
		}

		// Create new list with wrapped rows
		List<String[]> listWrappedRows = new ArrayList<>();
		for (String[] row : listRows) {
			// If any cell data is more than max length, then it will need extra row.
			boolean needExtraRow = false;  // True if any value is longer than max length
			int splitRow = 0;   // counter of split rows
			do {
				needExtraRow = false;
				String[] newRow = new String[columnLengths.size()];
				for (int j = 0; j < columnLengths.size(); j++) {
					if (row[j] == null)
						newRow[j] = "";
					// If data is less than max length, use that as it is
					else if (row[j].length() < maxLength)
						newRow[j] = splitRow == 0 ? row[j] : "";
					else if ((row[j].length() > (splitRow * maxLength))) {
						// Value longer than max length, so crop it at max length
						// Remainder will be part of next row
						int end = row[j].length() > ((splitRow * maxLength) + maxLength)
								? (splitRow * maxLength) + maxLength
								: row[j].length();
						newRow[j] = row[j].substring((splitRow * maxLength), end);
						needExtraRow = true;
					} 
					else 
						newRow[j] = "";
				}
				listWrappedRows.add(newRow);
				if (needExtraRow) {
					splitRow++;
				}
			} while (needExtraRow);
		}
	 	
		// Based on column lengths, create the print format and a horizontal line between header and results
		boolean leftJustified = true;
		final StringBuilder formatString = new StringBuilder("");
		String flag = leftJustified ? "-" : "";
		String line = "+";
		for (String col : columnNames) {
			int len = (columnLengths.get(col) < maxLength ? columnLengths.get(col) : maxLength );
			formatString.append("| %" + flag + len + "s ");
			line += "-";
			for (int j = 0; j < len ; j++)
				line += "-";
			line += "-+";
	    }
		line += newline;
		formatString.append("|" + newline);

		// Next to attribute name, also mention the applied weight
		for (ResultFacet col:results[0].getAttributes()) {
			columnNames.set(columnNames.indexOf(col.getName()), col.getName() + "[" +  df.format(weights.get(col.getName())) +"]");
		}
		
		// Print the formatted data
		// HEADER
		txtResult.append(line);
		txtResult.append(String.format(formatString.toString(), columnNames.toArray())); 
		txtResult.append(line);
		// ROWS
		for (String[] row: listWrappedRows) {   
			txtResult.append(String.format(formatString.toString(), row));
		}
		//STATISTICS
		txtResult.append(line);
		txtResult.append(listRows.size() + " rows selected in " + responseTime + " sec." + newline);
		
		return txtResult.toString();
	}

	/**
	 * Converts the given value into a string.
	 * @param val  A value of diverse data type.
	 * @return  A string value.
	 */
	private String getString(Object val) {
		if (val == null)
			return "";
		else if (val instanceof String[])   // Handle string array values
			return Arrays.toString((String[])val);
		else  // All other values
			return val.toString();
	}
	
}
