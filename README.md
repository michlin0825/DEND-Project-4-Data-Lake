### Project Background

* Sparkify provides music streaming to end users. Data of song details and user activities is captured as JSON files.

* AWS S3 is designated as data lake, enabling persistent data storage and ad hoc interactive queries.  

* We use pyspark to ingest data from data source, clean and load data into fact table, and dimension tables, and save resulting data back to S3. 


### AWS Setup
 
* Create an IAM user with programmatic access to S3 bucket.  

* Save IAM user access key id and secret access key to dl.cfg file, and load it on run time to gain access to AWS resources.  


### Data Sources

* Song Dataset, with metadata about a song and the artist of that song.

* Log Dataset, with simulated app activity logs.


### Fact Table

* songplays: `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`


### Dimension Tables

* users:
`user_id, first_name, last_name, gender, level`

* songs:
`song_id, title, artist_id, year, duration`

* artists:
`artist_id, name, location, lattitude, longitude`

* time:
`start_time, hour, day, week, month, year, weekday`



### ETL Scripts

* etl.ipynb: create jupyter notebook with pipeline prototype, test validity of ETL script, and add code to etl.py.

* etl.py: load data from s3 to pysaprk, clean, transform, and insert into fact table and dimension tables, and finally save resulting data in parquet back to S3. 



### Execution Steps

* update AWS IAM credential in dl.cfg file. 

* open terminal, and run `python etl.py`

