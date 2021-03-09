import requests

# Request to the web service to create a new instance of SimSearch
url = 'http://localhost:8090/simsearch/api/index'

# Example JSON specification of the multiple data sources with queryable attributes regarding the same entities having common identifiers.
# Three different types of data sources (each providing one or multiple queryable attributes) are specified in this example: 
#   (1) JDBC conncetion to a PostgreSQL/PostGIS database; 
#   (2) a local CSV file; and 
#   (3) An ElasticSearch REST API service hosted at a remote server.
index = {'sources':[{'name':'localPostGISdatabase','type': 'jdbc','driver': 'org.postgresql.Driver','url': 'jdbc:postgresql://localhost:5432/myDatabase','username':'postgresUserName','password':'postgresPassword','encoding':'UTF-8'}, {'name':'localPath1','type':'csv','directory':'./test/'}, {'name':'remoteElasticSearchAPI','type':'restapi','url':'http://remoteHostIPaddress:9200/myIndexName/_search?pretty','username':'elasticUserName','password':'elasticPassword'}], 'search':[ {'operation':'spatial_knn','source':'localPath1','dataset':'dataset1.csv','header':'true','search_column':'WKT'}, {'operation':'numerical_topk','source':'localPath1','header':'true','dataset':'dataset1.csv','search_column':'pos_sentiment'}, {'operation':'numerical_topk','source':'localPath1','dataset':'dataset1.csv','header':'true','search_column':'neg_sentiment'}, {'operation':'numerical_topk','source':'remoteElasticSearchAPI','dataset':'companies','key_column':'id','search_column':'employees'}, {'operation':'categorical_topk','source':'localPath1','dataset':'dataset2.csv','search_column':'persons'}, {'operation':'numerical_topk','source':'remoteElasticSearchAPI','dataset':'companies','key_column':'id','search_column':'revenue'}, {'operation':'spatial_knn','source':'localPostGISdatabase','dataset':'companies','search_column':'location'}, {'operation':'categorical_topk','source':'localPostGISdatabase','dataset':'companies','search_column':'keywords'}]}

# No API key is required when submitting this request to create a new instance
# A new API key will be generated once this request completes successfully
headers = {'Content-Type' : 'application/json'}

# Post a request with these parameters
response = requests.post(url, json=index, headers=headers)

# Provide the resulting message
# IMPORTANT! Save the API key return with this message and use it in all subsequent requests against this instance of SimSearch
print(response.json())