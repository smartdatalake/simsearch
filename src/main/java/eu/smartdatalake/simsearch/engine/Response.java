package eu.smartdatalake.simsearch.engine;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Generic response involving notifications on submitted requests.
 */
@JsonInclude(Include.NON_NULL)    // Ignore NULL values when issuing the response
public class Response {

	private String notification;
	private Optional<String> api_key = Optional.empty();
	
	/**
	 * Specifies notification(s) concerning a request or its results.
	 * @param notification  A string to notify the user of any issues regarding a request or its results.
	 */
	public void setNotification(String notification) {
		this.notification = notification;
	}
	
	/**
	 * Appends extra notification(s) concerning a request or its results.
	 * @param notification  A string to notify the user of any issues regarding a request or its results.
	 */
	public void appendNotification(String notification) {
		
		if (this.notification == null)
			this.notification = notification;
		else
			this.notification += " " + notification;
	}
	
	/**
	 * Provides any notification(s) concerning a request or its results.
	 * @return  A string to notify the user of any issues regarding a request or its results.
	 */
	public String getNotification() {
		return notification;
	}

	/**
	 * Provides the API key to be used for connection to this running SimSearch instance.
	 * @return  The API key used by this SimSearch instance.
	 */
	public String getApiKey() {
		// Return the API key, if specified
		if (api_key.isPresent())
            return api_key.get();
		
		return null;
	}

	/**
	 * Specifies the API key that will be used in the user notification once this SimSearch instance is created. 
	 * @param api_key  The API key to be used for connecting to the SimSearch instance.
	 */
	public void setApiKey(String api_key) {
		this.api_key = Optional.ofNullable(api_key);
	}
	
}
