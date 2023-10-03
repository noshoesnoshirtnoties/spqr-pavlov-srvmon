# spqr-pavlov-srvmon
script to monitor the spqr pavlov server, running as systemd service.

meant to work in coop with https://github.com/noshoesnoshirtnoties/spqr-servus-publicus

## usage description
* clone this repo to your workstation
* copy config.json.example to config.json and edit the latter to your liking
* prepare a server so you can access it as root via ssh
* prepare a server with a mysql/mariadb and load the database from spqr-database.sql
* use deploy.sh like this: ./deploy.sh -d [hostname-or-ip] -u [ssh-and-scp-user] -v
* check for errors - the service should be up and running now as spqr-pavlov-srvmon.service
* journal only receives info for service status changes
* app log is only written to file (spqr-pavlov-srvmon.log)

## requirements
* pip modules
  * async-pavlov
  * mysql-connector

## todo (aside from finding and fixing bugs and improving the code in general)
* check stability/packetloss with auto-kick-high-ping
* extended playerstats (DM + TDM)
  * pull steamusers details
  * ace-detection for playerstats
* make env a param (main.py)
* zip/unzip files (deploy.sh)
* remove requirement to access the server as root (deploy.sh)