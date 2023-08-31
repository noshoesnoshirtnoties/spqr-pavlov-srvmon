--  mysql database template for spqr
DROP DATABASE IF EXISTS spqr;
CREATE DATABASE spqr;
USE spqr;
CREATE TABLE register (
  id INT NOT NULL AUTO_INCREMENT,
  discordusers_id INT NOT NULL,
  steamusers_id INT NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE discordusers (
  id INT NOT NULL AUTO_INCREMENT,
  discordid VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE steamusers (
  id INT NOT NULL AUTO_INCREMENT,
  steamid64 VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE steamusers_details (
  id INT NOT NULL AUTO_INCREMENT,
  steamusers_id INT NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE stats (
  id INT NOT NULL AUTO_INCREMENT,
  steamusers_id INT NOT NULL,
  kills INT NOT NULL,
  deaths INT NOT NULL,
  average FLOAT NOT NULL,
  score INT NOT NULL,
  ping FLOAT NOT NULL,
  servername VARCHAR(255) NOT NULL,
  playercount VARCHAR(255) NOT NULL,
  mapugc VARCHAR(255) NOT NULL,
  gamemode VARCHAR(255) NOT NULL,
  matchended BOOLEAN NOT NULL,
  teams BOOLEAN NOT NULL,
  team0score INT NOT NULL,
  team1score INT NOT NULL,
  timestamp DATETIME NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE ranks (
  id INT NOT NULL AUTO_INCREMENT,
  steamusers_id INT NOT NULL,
  rank INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE pings (
  id INT NOT NULL AUTO_INCREMENT,
  steamid64 VARCHAR(255) NOT NULL,
  ping FLOAT NOT NULL,
  timestamp DATETIME NOT NULL,
  PRIMARY KEY (id)
);