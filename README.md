# Introduction
***
This repo is for a make believe company, Sparkify, that provides a music streaming service.
We have metadata on the various songs that are supported by the platform, as well as usage logs by the users of the platform.

The S3 bucket for the song metadata is:

`s3://udacity-dend/song_data`

The S3 bucket for the user log data is:

`s3://udacity-dend/log_data`


In this project, we will:
* launch a Spark cluster on AWS EMR
* read that S3 data using Spark
* split the data into 5 tables
* transform the data appropriately
* save the data down in parquet files in separate directories in an output S3 bucket
# The Data
***
A single song metadata file looks like this:
* `TRAABJL12903CDCF1A.json`:
		* "num_songs": 1  
		* "artist_id": "ARJIE2Y1187B994AB7"  
		* "artist_latitde": null  
		* "artist_longitude": null  
		* "artist_location": ""  
		* "artist_name": "Line Renaud"  
		* "song_id": "SOUPIRU12A6D4FA1E1"  
		* "title": "Der Kleine Dompfaff"  
		* "duration": 152.92036  
		* "year": 0  

A single user songplay event file looks like this:
* `2018-11-12-events.json`:
		* "artist": "Pavement"  
		* "auth": "Logged In"  
		* "firstName": "Sylvie"  
		* "gender": "F"  
		* "itemInSession": 0  
		* "lastName": "Cruz"  
		* "length": 99.16036  
		* "level": "free"  
		* "location": "Washington-Arlington-Alexandria, DC-VA-MD-WV"  
		* "method": "PUT"  
		* "page": "NextSong"  
		* "registration": 1.540266e+12  
		* "sessionId": 345  
		* "song": "Mercy: The Laundromat"  
		* "status": 200  
		* "ts": 1541990258796  
		* "userAgent": "Mozzilla/5.0 (Macintosh; Intel Mac OS X 10_9_4.."  
		* "userId": 10  

# Files
***
In this repo, there is the file `etl.py`.
This file will contain the PySpark code that will:
* read data from S3
* process the data from S3
* write the tables back to S3
# Tables
***
The output tables of this repo will be:

* songplays - log data having `page='NextSong'`:
		* songplay_id  
		* start_time  
		* user_id  
		* level  
		* song_id  
		* artist_id  
		* session_id  
		* location  
		* user_agent  
* users:
		* user_id  
		* first_name  
		* last_name  
		* gender  
		* level  
* songs:
		* song_id  
		* title  
		* artist_id  
		* year  
		* duration  
* artists:
		* artist_id  
		* name  
		* location  
		* latitude  
		* longitude  
* time - timestamps of records in songplays broken out into various time units:
		* start_time  
		* hour  
		* day  
		* week  
		* month  
		* year  
		* weekday  
