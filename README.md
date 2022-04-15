# Elasticsearch API Automation

## Project Overview

A class for accessing Elasticsearch API and automating the retrieved data.

## Main Features
* Authenticate with Elasticsearch.

* Verify the connection by pinging to Elasticsearch server.

* Enable logging to track the requests made to Elasticsearch server.

* Search Elasticsearch for any type of data using JSON queries.

* Clean the timestamp in the data retrieved by Elasticsearch.

## Elasticsearch and Kibana Limitations
Both Elasticsearch and Kibana limit the results to maintain a good performance in the cluster.

I used to run a query to get 1 day of data inside a visualization on Kibana, with time interval step = 1 minute. But Kibana forced a larger time step of 10 minutes because "data was too much".

The same goes with Elasticsearch, but unlike Kibana, Elasticsearch isn't shy to throw an error telling you that data is too much.


## Partial Solution
The solution that I implemented in this tool is that it tries to run the query with the minimum time interval step, and if it encounters an exception of Elasticsearch indicating the data is too much; it increases the time step by 1 minute, then retry again, and repeat till we get a successful search process.

While Kibana forces a time interval of 10 minutes to display one day of data, the tool can acquire up to 4 days of data from the same index, with time interval = 1 minute!

The rest of the data analysis and capacity assessment processes accommodate the dynamic time interval step with no impact on the analysis at all.

## What Software Did I Need?

To complete this project, I used the following software:

* Python [pandas, NumPy, matplotlib, elasticsearch, time, datetime, sys]
* A text editor: VS Code
* Version Control with Git

## About The Author

* Author: Mohamed Abdel-Gawad Ibrahim
* Contact: muhammadabdelgawwad@gmail.com
* Phone: +201147821232
