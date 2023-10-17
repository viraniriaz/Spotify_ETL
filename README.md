# Spotify_ETL
For this project, the data was collected from Spotify using Spotify’s API and ‘spotipy’ library. The data is on Best Hindi songs 2023 in United States (ranked weekly)

https://open.spotify.com/playlist/0zc6Hq9OIAengtGG6a3lfs

Services User: Google Cloud Storage, IAM and admin, Google Function, Cloud Scheduler, Google Pub/Sub, Snowflakes

Process:

  1. Created Spotify developer account to generate API keys.
  2. Assigned roles and policies for the services used thorough IAM.
  3. Created custom python package and generated custom layer to support spotipy library in GCP in file name requirement.txt.
  4. Coded function to extract raw data using Spotify API and ‘spotipy’ python library in Google function
  5. Automated data extraction using Cloud Scheduler for a specified time and store data in ‘raw_data’ folder in Cloud Storage bucket.
  6. Executed transform function to convert the raw data into 3 csv files – album, artist, songs.
  7. Automated the process of transform function and to copy the transformed files into ‘processed_data’ folder and delete the data from ‘raw_data’ folder.
  8. Designed tables for album, artist and songs in snowflakes.
  9. Created storage integration to access the data from cloud storage. Made file format for csv file.
  10. Created snowpipe by leveraging google Pub/Sub for automatically retrieving of the data when the raw_file appear in raw_data folder.
  11. Queried the tables

It was a great experience using API to collect data and then transform this raw data into legible data using GCP. This was a fun project as I was able to work with real time data which keeps on updating.

Spotify Developer : https://developer.spotify.com
     
      
# Architecture Diagram


<img width="515" alt="image" src="https://github.com/viraniriaz/Spotify_ETL/assets/82742908/cbc62ff3-e71c-454b-bdc9-ded839f20758">
