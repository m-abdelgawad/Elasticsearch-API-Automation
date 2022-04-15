# Program Name	: Elasticsearch API Automation
# Version       : 2.0
# Date          : 29 Sep 2021
# Author     	: Mohamed Abdel-Gawad Ibrahim
# Email         : muhammadabdelgawwad@gmail.com


# Pandas is an open source data analysis and manipulation package
import pandas as pd

# import Elasticsearch library to connect to Elasticsearch
from elasticsearch import Elasticsearch


class ElasticSearch:
	"""Connect to Elasticsearch and run JSON queries, then convert them to
	Pandas dataframe objects.
	
	Please note that: - This class searches for two levels of aggregation only; first level should be named by 'flag1'
	and second level should be named 'flag2'.
	"""
	
	def __init__(self, ip, port, username, password, tag):
		"""Instantiate an instance of ElasticSearch

		Args:
			ip ([type]): [description]
			port ([type]): [description]
			username ([type]): [description]
			password ([type]): [description]
			tag (str) : A tag to mark the object instance
		"""
		
		# Create the host string
		host = 'http://{}:{}'.format(ip, port)
		
		# create a client instance of Elasticsearch
		# HTTP authentication uses the http_auth parameter by passing in a
		# username and password within a tuple:
		self.es = Elasticsearch(
			host,
			http_auth=(username, password)
		)
		
		# Set object tag
		self.tag = tag
	
	def verify_connection(self):
		"""Verify the connection with Elasticsearch.
		"""
		
		if not self.es.ping():
			print('Failes')
			raise ValueError("Connection failed")
		else:
			print('Success')
	
	def enable_logging(self):
		"""Enable logging in the terminal for debugging
		"""
		es_trace_logger = logging.getLogger('elasticsearch.trace')
		es_trace_logger.setLevel(logging.DEBUG)
		handler = logging.StreamHandler()
		es_trace_logger.addHandler(handler)
	
	def search_elastic(self, index, query, field, interval_step):
		"""Search Elasticsearch then convert the results into a Pandas
		dataframe.

		Args:
			index (str): The index we want to query
			query (str): The JSON query
			field (str): The field we want to use it in the aggregation
			interval_step (int): The minimum time interval step we want in
				our search results
		"""
		
		# Set a flag to indicate a failed search operation
		is_failed = True
		
		# Save the raw query before we manupilate its time interval step
		raw_query = query
		
		# Loop till we get a successful search
		while is_failed:
			
			# Try to search with the current interval step
			try:
				
				# Edit the query with the current interval step
				exec_query = raw_query.replace('<step>', str(interval_step))
				
				# Search Elasticsearch and return the results in JSON format
				search_results = self.es.search(
					index=index,
					body=exec_query,
					request_timeout=60,
					size=1)
				
				# If we passed the previous step, set the failed flag to false;
				# which will break the while loop and continue the program
				# execution
				is_failed = False
			
			# If the search operation failed, we check if the error caused by
			# too many data; so we increase the interval step by 1 minute; then
			# we loop again and try to execute the search.
			# If the error is caused by a failed connection; we display a short
			# message indicating the error.
			# If any other error, we print the error message
			except Exception as e:
				
				# Check if the error was because too many data; we increase
				# the time interval step
				if str(e) == "TransportError(503, " \
				             "'search_phase_execution_exception')":
					
					# print a warning indicating the error
					print('\nToo many data with time step = {} minutes'.format(
						interval_step))
					print("increasing the time step...\n")
					
					# Increment the time interval step
					interval_step += 1
				
				# Check if the error is cause by a failure in the connection
				# then display a warning to the user
				elif 'ConnectionError' in str(e):
					print("\nConnection Error: " \
					      "Failed to establish a new connection")
					print("Please try to connect to the VPN network of "
					      "Elasticsearch\n")
					
					# Hold the program from closing to make sure the user
					# sees the error type
					input("Please enter any character to exit: ")
					
					# Then exit
					exit()
				
				# If we encounter any other error, we print the exception
				else:
					print('Error: <{}>'.format(str(e)))
					
					# Hold the program from closing to make sure the user
					# sees the error type
					input("Please enter any character to exit: ")
					
					# Then exit
					exit()
		
		# After executing the search, print the time interval that was used
		# just to align with the user what
		print("\n", "-" * 50, "\n",
		      'Final Interval Step = {} minutes'.format(interval_step),
		      "\n", "-" * 50)
		
		self.interval_step = interval_step
		
		# Start converting the JSON results into a Pandas Dataframe
		
		# Best way to create a dataframe row by row is to through a list of
		# dictionaries
		
		# So we create an empty list and dictionary
		row = {}
		rows = []
		
		# Loop over the first level of the results
		for prim_dict in search_results['aggregations']['flag1']['buckets']:
			
			# Extract the timestamp from the results
			timestamp = prim_dict["key_as_string"]
			
			# Loop over the second level of results
			for sec_dict in prim_dict['flag2']['buckets']:
				# Extract the aggregated count
				requests = sec_dict['doc_count']
				
				# Extract the field string value
				node = sec_dict['key']
				
				# Construct the row record of the dataframe we want to create
				row = {
					'timestamp': timestamp,
					str(field): node,
					'requests': requests
				}
				
				# Append the row record to the list of dictionaries
				rows.append(row)
		
		# Convert the list of dictionaries to a dataframe
		self.df = pd.DataFrame(rows)
	
	def clean_timestamp(self):
		"""By default, Elasticsearch responds with the following timestamp
		format:
		2021-09-27T02:01:00.000+02:00
		So, this method convert the previous format, to multiple data and time
		columns
		"""
		
		# Remove the timezone of the timestamps
		self.df['timestamp'] = self.df['timestamp'].dt.tz_localize(None)
		
		# Extract the 'year' column from the 'timestamp' column
		self.df['year'] = self.df['timestamp'].dt.year.astype('Int64')
		
		# Extract the 'month' column from the 'timestamp' column
		self.df['month'] = self.df['timestamp'].dt.month.astype('Int64')
		
		# Extract the 'day' column from the 'timestamp' column
		self.df['day'] = self.df['timestamp'].dt.day.astype('Int64')
		
		# Extract the 'hour' column from the 'timestamp' column
		self.df['hour'] = self.df['timestamp'].dt.hour.astype('Int64')
		
		# Extract the 'minute' column from the 'timestamp' column
		self.df['minute'] = self.df['timestamp'].dt.minute.astype('Int64')
