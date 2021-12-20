package eu.smartdatalake.simsearch;

import eu.smartdatalake.simsearch.engine.Response;
import io.swagger.annotations.ApiModelProperty;

/**
 * Auxiliary class holding the main settings of the launched SimSearch instance.
 * Modeled like the ElasticSearch settings response for conformity in REST API connections.
 */
public class InstanceSettings extends Response {

	@ApiModelProperty(required = true, value = "Main information concerning the running SimSearch instance")
	public Settings settings;
	
	/**
	 * Constructor
	 */
	public InstanceSettings() {
		settings = new Settings();
	}
	
	public class Settings {
		
		@ApiModelProperty(required = true, value = "Dictionary of main parameters concerning the running SimSearch instance")
		public Index index;      
		
		/**
		 * Constructor
		 */
		public Settings() {
			index = new Index();
		}
		
		/**
		 * Auxiliary class that imitates a similar structure in ElasticSearch settings.
		 */
		public class Index { 
		
			private String provided_name;
			private String max_result_window;
			private String creation_date;
			private long query_timeout;    				// Max execution time for ranking (in milliseconds)
			public boolean isSimSearchInstance = true;  // By default set to true for a running SimSearch instance.
			
			/**
			 * Constructor
			 */
			public Index() {}
			
			/**
			 * Returns the name of the running instance.
			 * @return  A string with the name of the SimSearch instance.
			 */
			public String getProvidedName() {
				return provided_name;
			}
			
			/**
			 * Sets the name of the running instance.
			 * @param provided_name  A string to be used as the name of the SimSearch instance.
			 */
			public void setProvidedName(String provided_name) {
				this.provided_name = provided_name;
			}
	
			/**
			 * Returns the maximum number of results that a SimSearch request can provide.
			 * @return  A string indicating the max number of query results.
			 */
			public String getMaxResultWindow() {
				return max_result_window;
			}
	
			/**
			 * Sets the maximum number of results that a SimSearch request can provide.
			 * @param max_result_window  A string indicating the max number of query results.
			 */
			public void setMaxResultWindow(String max_result_window) {
				this.max_result_window = max_result_window;
			}
	
			/**
			 * Indicates when this SimSearch instance was launched. 
			 * @return  A timestamp value (epoch in milliseconds).
			 */
			public String getCreationDate() {
				return creation_date;
			}
	
			/**
			 * Sets the date when this SimSearch instance was launched. 
			 * @param creation_date  A timestamp value (epoch in milliseconds).
			 */
			public void setCreationDate(String creation_date) {
				this.creation_date = creation_date;
			}

			/**
			 * Indicates the max execution time available for ranking in a submitted search query.
			 * @return  An integer representing the max allowed time (in milliseconds).
			 */
			public long getQueryTimeout() {
				return query_timeout;
			}

			/**
			 * Sets the max execution time available for ranking in a submitted search query.
			 * @param query_timeout  An integer representing the max allowed time (in milliseconds).
			 */
			public void setQueryTimeout(long query_timeout) {
				this.query_timeout = query_timeout;
			}
		}	
	}
	
}
