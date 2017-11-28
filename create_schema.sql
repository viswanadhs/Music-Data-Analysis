CREATE DATABASE IF NOT EXISTS project;

USE project;

CREATE TABLE IF NOT EXISTS top_10_stations
(
station_id VARCHAR(50),
total_distinct_songs_played INT,
distinct_user_count INT
);

CREATE TABLE IF NOT EXISTS song_duration
(
user_id VARCHAR(50),
user_type VARCHAR(50),
song_id VARCHAR(50),
artist_id VARCHAR(50),
total_duration DOUBLE
);
CREATE TABLE IF NOT EXISTS connected_artists
(
artist_id VARCHAR(50),
total_distinct_songs INT,
user_count INT
);

CREATE TABLE IF NOT EXISTS top_10_royality_songs
(
song_id VARCHAR(50),
artist_id VARCHAR(50),
duration DOUBLE
);

CREATE TABLE IF NOT EXISTS top_10_unsubscribed_users
(
user_id VARCHAR(50),
song_id VARCHAR(50),
artist_id VARCHAR(50),
duration DOUBLE
);

