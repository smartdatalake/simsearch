package eu.smartdatalake.simsearch.manager.ingested.temporal;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;

/**
 * Parser for date/time values, optionally applying a user-specified format.
 */
public class DateTimeParser {

	// Most common patterns of dates and times to apply in parsing 
	String[] datePatterns = new String[] { "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssX", "yyyy-MM-dd HH:mm:ss", "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss.SSX", "yyyy-MM-dd", "dd/MM-yyyy", "dd/MM/yyyy", "yyyy-MM", "YYYY", "HH:mm:ss"};
	
	String msg = null;
	
	/**
	 * Default constructor
	 */
	public DateTimeParser() {
	}

	/**
	 * Constructor employing a user-specified format for date/time values.
	 * @param format  Format of date/time values.
	 */
	public DateTimeParser(String format) {

		// Include this extra format into those applied
		if (!Arrays.stream(datePatterns).anyMatch(format::equals))
			ArrayUtils.add(datePatterns, format);

	}
	
	/**
	 * Converts a date/time value to a double value (epoch in milliseconds). 
	 * @param strDate  A date/time value in a typical format.
	 * @return  The corresponding epoch (milliseconds in the decimal part).
	 */
	public Double parseDateTimeToEpoch(String strDate) {
		
		if (!strDate.trim().isEmpty()) {
			Date date;
			try {			
				date = DateUtils.parseDate(strDate, datePatterns);
				return date.toInstant().toEpochMilli()/1000.0;   //milliseconds stored in the decimal part
			} catch (ParseException e) {
				// Message that may be issued as notification
				msg = "Malformed or unparsable date/time value: " + strDate;
			}
		}

		return null;
	}
	
	
	/**
	 * Provides a notification regarding any problems in parsing the specified temporal value.
	 * @return  A message to be reported.
	 */
	public String getNotification() {
		return msg;
	}
	
}
