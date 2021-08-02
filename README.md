# Data Lake and ETL with pyspark

### Table of Contents
- Introduction to this project
- Files in this repository
- How to run the python scripts
- Database schema design and ETL process
- Data analysis

### Files in this repository

There are two main files in this repository:

- `/src/notebook/proof_of_concept.ipynb`: A walk through the ETL process
- `/src/etl.py`: a programmatic representation of the ETL process based on the "PoC", the proof of concept.

### How to run the python scripts

Before you run the scripts, you need to update the file `/src/dl.cfg` with your AWS key and secret.

To run the PoC or `etl.py` you need to have a spark connection. You can use `docker-compose up` from the root directory to create a local spark cluster and connect to it with jupyter lab. You can then either open the PoC-notebook or you can run `etl.py` from the jupyter lab shell. 

### Introduction: Sparkify Audio Streaming, User Data and AWS.

Sparkify is a (fictional) startup that offers music streaming services. They accumulate 
data but do not yet leverage them. In this project I build an
ETL pipeline that loads data from S3 to Redshift. A walk through a concept in [a jupyter notebook.](./notebooks/Sample_Loads.ipynb) 

In order to perform the ETL process, I connect to the S3 bucket using Python and boto3. I then copy all json files that are stored in that bucket to AWS Redshift.
The data first lands to a staging table. From there the ETL processes transform the data by using SQL to SQL design in Redshift. 

With the data model and the Sparkify will have the ability to perform several analyses.
For instance, Sparkify will gain the ability to understand which songs are being played
the most, which OS and browser their users use or how the distribution between free
and paid users is.

Now that we have learned more about Sparkify and the purpose of this project, 
I will present more details about the data in the next chapter.

### Database Schema Design and ETL Process

The data is stored as json files in AWS S3. The buckets are:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

The first ETL step is to copy all these files into Redshift. Here is the data model for the two staging tables:

![staging tables](documentation/images/original_data.png)

The next steps of the ETL is to distribute these columns into their respective dimensions tables. 

__events data:__
- users
- time

__song data:__
- artists
- songs

![The Four Dimension Tables](documentation/images/schemas/dimension_tables.png)

In the center of the dimension table will be the facts table. together, these form
a star schema.

#### The Facts Table "Songplays" and the Star Schema

The `songplays` table is in the center of the four dimension tables. It gets its
data from the original `log_data` as well as the `user_id` from the `users` and the 
`artist_id` from the `artists` table. The `songplays` table references every dimension 
table with a corresponding foreign key. The following diagram shows the relation
of every column of the facts table:

![The Star Schema](documentation/images/schemas/songplays.png)

#### Rationale and Data Analysis

With the facts table, sparkify has now many uses for business analysis. Here are some questions that can
be answered now:

- At what hours of the day are the most songplays?
- What artists are being played the most?
- What browser / operating system are being used?
- What is the distribution between free and paid users?

Let's look at the analysis of these questions.

![songplays by hour of day](documentation/data_analysis/songplays_by_hour_of_day.png)
_Most listeners are active around afternoon to evening._

![Most played artists](documentation/data_analysis/most_played_artists.png)
_The top three most played artists are: 1 Muse, 2 Radiohead, 3 Coldplay._

![user agents](documentation/data_analysis/user_agents.png)
_Apparently most users listen on MacOS._

![Free and Paid Songplays](documentation/data_analysis/free_and_paid_songplays.png)
_There are way more paid users listening on Sparkify then free users._

