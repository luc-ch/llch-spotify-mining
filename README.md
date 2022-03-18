# SPOTIFY-MINING

With this project, we are building a Postgres database with:

- The historical set of Spotify Charts of the daily 200 top songs for each country where Spotify has worked in since January, 2017.
- Each track's basic data consisting of its metadata.
- Each track's artists.
- Each artist's genres.
- Each track's features.
- Each track's audio analysis.
- Each track's lyrics.

A Postgres DB must be up and running, and a file called `./config.ini` must have the following data:

```ini
[db]
connection_string=postgresql://x:x@x/x
host=x
port=0
database=x
user=x
password=x

[token]
spotify_client_id=x
spotify_client_secret=x
spotify_user_id=x
genius_secret=x-x

[general]
persist_to_file=False
dataset_folder=./datasets
```
