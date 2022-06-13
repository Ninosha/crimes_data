# 

Python project fully working on Google Cloud Platform using cloud 
functions, cloud scheduler, cloud run, bigquery.
1. First Cloud Function is used to fetch daily data for three months 
from bigquery crimes public dataset. Saves data daily in bigquery 
dataset and creates views according to filtered columns.
2. contains API using FAST API run with cloud run. has options to read 
table and view table, insert/update/delete rows/values in table.
3. Second cloud function is used to communicate with API via pub/sub to 
copy the table user wants to add/update/delete rows/values.



## Usage

To test the API go to URL: 
https://crimesapi-eneebd45xa-ew.a.run.app/docs

Read request can get first ten rows in form of python dictionary.
Follow the docs to use the CRUD on bigquery tables.
{"unique_key":3, 	"case_number":"asdj8", "date":"2020-01-01 12:00:00 UTC", 
	block	iucr	primary_type	description	location_description	arrest	domestic	beat	district	ward	community_area	fbi_code	x_coordinate	y_coordinate	year	updated_on	latitude	longitude	location}