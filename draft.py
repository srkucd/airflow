import psycopg2

staging_sample = 's3://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json'

staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"

song_table_drop = "DROP TABLE IF EXISTS songs"

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 num_songs INTEGER,
                                 artist_id VARCHAR,
                                 artist_lattitude FLOAT,
                                 artist_longitude FLOAT,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration FLOAT,
                                 year INTEGER)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR,
                        artist_id VARCHAR,
                        year INTEGER,
                        duration FLOAT);
""")

staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         CREDENTIALS 'aws_iam_role={}'
                         REGION 'us-west-2'
                         FORMAT AS JSON 'auto'
""").format(staging_sample, 'arn')

song_table_insert = ("""INSERT INTO songs(song_id, 
                                          title, 
                                          artist_id, 
                                          year, 
                                          duration)
                        SELECT DISTINCT song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM staging_songs
""")

conn = psycopg2.connect("""host=host
                           dbname=dev 
                           user=awsuser
                           password=password
                           port=5439""")

cur = conn.cursor()

cur.execute(song_table_drop)
conn.commit()
cur.execute(song_table_create)
conn.commit()
cur.execute(staging_songs_table_drop)
conn.commit()
cur.execute(staging_songs_table_create)
cur.execute(staging_songs_copy)
conn.commit()
cur.execute(song_table_insert)
conn.commit()