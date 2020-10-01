package eu.smartdatalake.simsearch;

/**
 * Generic response involving notifications on submitted requests.
 */
public class Response {

	private String notification;
	
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
	
}
