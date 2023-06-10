## Running this code 

1. Fill in `dwh.cfg` with your AWS information, cluster information (including host and DB login), and an IAM_ROLE that allows you to access your cluster. 
2. Run `python create_tables.py` to create all tables 
3. Run `python etl.py` to stage and load data, and run sample analysis. 

NOTE: This code currently only loads 1 month of songplay data and a small subset of song data, for efficiency. This can be modified by changing the s3 addresses in `sql_queries` to reference the entire dataset. Specifically, to do this, remove the `/2018/11` and `/A/A` suffixes where they appear in the `staging_events_copy` and `staging_songs_copy` queries. Note that you will need a large cluster and patience to run the full dataset! 

## Staging table design 

Data is staged in two tables: `staging_songs`, which holds data from the million songs database, and `staging_events`, which holds data describing app transactions. We'll be pulling data from both tables to produce the analysis tables (described below). We'll align data between the tables using the song title, so we use it as the distribution key for both tables. This will make copying to the final `songplay` table more efficient. 

## Analysis table design 

The analysis table design is provided. It is a star schema design, with the `songplay` table as the fact table and `users`, `artists`, `songs`, and `times` as the dimension tables. Since we do not know ahead of time how many users and artists there will be relative to the number of songplays, we will leave the distribution strategy as 'auto' -- this will automatically distribute smaller tables (eg, if there are only a few users, the `users` table will be copied to all nodes) and distribute larger tables (eg, if there are many artists, the table will automatically be distributed).

## Exploratory queries 

We provide 2 example queries, one that joins the `artists` and `songplay` tables to gather the 5 most listened to artists, and one that uses only the `songplay` table to gather the 5 most common user locations. 


