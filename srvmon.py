import os
import re
import sys
import json
import time
import logging
import asyncio
from pavlov import PavlovRCON
import mysql.connector
from datetime import datetime,timezone

def run_srvmon(meta,config):
  # set vars
  debug=config['debug']
  target_log=config['logfile_path']
  rconip=config['rconip']
  rconport=config['rconport']
  rconpass=config['rconpass']
  keywords=[
    'Rotating map',
    'LogLoad: LoadMap',
    'Updating blacklist'
    'StartPlay',
    '"State":',
    'Preparing to exit',
    'LogHAL',
    'Server Status Helper',
    'Rcon: User',
    'SND: Waiting for players',
    'long time between ticks',
    'Login request',
    'Client netspeed',
    'Join request',
    'Join succeeded',
    'LogNet: UChannel::Close',
    '"Killer":',
    '"KillData":',
    '"KillerTeamID":',
    '"Killed":',
    '"KilledTeamID":',
    '"KilledBy":',
    '"Headshot":',
    'LogTemp: Rcon: KickPlayer',
    'LogTemp: Rcon: BanPlayer',
    'BombData',
    '"Player":',
    '"BombInteraction":']

  # init logging
  if bool(debug)==True:
    level=logging.DEBUG
  else:
    level=logging.INFO
  logging.basicConfig(filename='spqr-pavlov-srvmon.log',filemode='a',format='%(asctime)s,%(msecs)d [%(levelname)s] srvmon: %(message)s',datefmt='%m/%d/%Y %H:%M:%S',level=level)
  logfile=logging.getLogger('logfile')

  # def funcs
  def logmsg(logfile,lvl,msg):
    lvl=lvl.lower()
    match lvl:
      case 'debug':
        logfile.debug(msg)
      case 'info':
        logfile.info(msg)
      case 'warn':
        logfile.warning(msg)
      case _:
        logfile.debug(msg)

  async def dbquery(config,logfile,query,values):
    logmsg(logfile,'debug','dbquery called')
    logmsg(logfile,'debug','query: '+str(query))
    logmsg(logfile,'debug','values: '+str(values))
    logmsg(logfile,'debug','len(values): '+str(len(values)))
    conn=mysql.connector.connect(
      host=config['mysqlhost'],
      port=config['mysqlport'],
      user=config['mysqluser'],
      password=config['mysqlpass'],
      database=config['mysqldatabase'])
    logmsg(logfile,'debug','conn: '+str(conn))
    cursor=conn.cursor(buffered=True)
    cursor.execute(query,(values))
    conn.commit()
    data={}
    data['rowcount']=cursor.rowcount
    logmsg(logfile,'debug','data[rowcount]: '+str(data['rowcount']))
    query_type0=query.split(' ',2)
    query_type=str(query_type0[0])
    logmsg(logfile,'debug','query_type: '+query_type)
    if query_type.upper()=="SELECT":
      rows=cursor.fetchall()
      logmsg(logfile,'debug','rows: '+str(rows))
      i=0
      data['rows']={}
      for row in rows:
        logmsg(logfile,'debug','row: '+str(row))
        data['rows'][0]=row
        i+=1
      i=0
      data['values']={}
      for value in cursor:
        logmsg(logfile,'debug','value: '+str(value))
        data['values'][0]=format(value)
        i+=1
    else:
      data['rows']=False
      data['values']=False
    logmsg(logfile,'debug','data: '+str(data))
    cursor.close()
    conn.close()
    logmsg(logfile,'debug','conn and conn closed')
    return data

  async def rcon(config,rconcmd,rconparams):
    logmsg(logfile,'debug','rcon called')
    logmsg(logfile,'debug','rconcmd: '+str(rconcmd))
    logmsg(logfile,'debug','rconparams: '+str(rconparams))
    conn=PavlovRCON(config['rconip'],config['rconport'],config['rconpass'])
    for rconparam in rconparams:
      rconcmd+=' '+str(rconparam)
    data=await conn.send(rconcmd)
    data_json=json.dumps(data)
    data=json.loads(data_json)
    logmsg(logfile,'debug','data: '+str(data))
    await conn.send('Disconnect')
    return data

  async def action(config,action):
    logmsg(logfile,'debug','action called')
    match action:
      case 'serverinfo':
        logmsg(logfile,'info','serverinfo called')
        command='ServerInfo'
        params={}
        data=await rcon(config,command,params)
        serverinfo=data['ServerInfo']

        if serverinfo['GameMode'].upper()=="SND": # demo rec counts as 1 in SND
          numberofplayers0=serverinfo['PlayerCount'].split('/',2)
          numberofplayers1=numberofplayers0[0]
          numberofplayers2=(int(numberofplayers1)-1)
          maxplayers=numberofplayers0[1]
          numberofplayers=str(numberofplayers2)+'/'+str(maxplayers)
        else:
          numberofplayers=serverinfo['PlayerCount']
        serverinfo['PlayerCount']=numberofplayers

        logmsg(logfile,'info','srvname:     '+str(serverinfo['ServerName']))
        logmsg(logfile,'info','playercount: '+str(numberofplayers))
        logmsg(logfile,'info','mapugc:      '+serverinfo['MapLabel'])
        logmsg(logfile,'info','gamemode:    '+serverinfo['GameMode'])
        logmsg(logfile,'info','roundstate:  '+serverinfo['RoundState'])
        logmsg(logfile,'info','teams:       '+str(serverinfo['Teams']))
        if serverinfo['Teams']==True:
          logmsg(logfile,'info','team0score:  '+serverinfo['Team0Score'])
          logmsg(logfile,'info','team1score:  '+serverinfo['Team1Score'])
      case 'autopin':
        logmsg(logfile,'info','autopin called')
        command='ServerInfo'
        params={}
        data=await rcon(config,command,params)
        serverinfo=data['ServerInfo']

        if serverinfo['GameMode'].upper()=="SND": # only autopin in SND / demo rec counts as 1 in SND
          numberofplayers0=serverinfo['PlayerCount'].split('/',2)
          numberofplayers1=(numberofplayers0[0])
          numberofplayers=(int(numberofplayers1)-1)
          logmsg(logfile,'info','current number of players: '+str(numberofplayers))

          limit=10
          if int(numberofplayers)>=limit:
            logmsg(logfile,'info','limit ('+str(limit)+') reached - setting pin 9678')
            command='SetPin'
            params={'9678'}
            data=await rcon(config,command,params)
          else:
            logmsg(logfile,'info','below limit ('+str(limit)+') - removing pin')
            command='SetPin'
            params={''}
            data=await rcon(config,command,params)
      case 'autokickhighping':
        logmsg(logfile,'info','autokickhighping called')
        command='InspectAll'
        params={}
        data=await rcon(config,command,params)
        inspectlist=data['InspectList']

        for player in inspectlist:
          logmsg(logfile,'debug','searching for other entries for this player in pings db')
          query="SELECT steamid64,ping,AVG(ping) as avg_ping,COUNT(id) as cnt_id FROM pings WHERE steamid64 = %s"
          values=[]
          values.append(str(player['UniqueId']))
          data=await dbquery(config,query,values)

          avg_ping=data['rows'][0][2]
          rowcount_pings=data['rows'][0][3]
          logmsg(logfile,'debug','found '+str(rowcount_pings)+' entries for player: '+str(player['UniqueId']))
          minentries=5
          if rowcount_pings>=minentries: # dont do anything, unless there are 5 entries for a player
            logmsg(logfile,'debug','rowcount ('+str(rowcount_pings)+') >= minentries ('+str(minentries)+')')
            pinglimit=59
            logmsg(logfile,'info','checking wether limit has been reached or not for player: '+str(player['UniqueId']))
            if int(avg_ping)>pinglimit:
              logmsg(logfile,'info','player ping average ('+str(int(avg_ping))+') exceeds the limit ('+str(pinglimit)+')')
              command='Kick'
              params={str(player['UniqueId'])}
              data=await rcon(config,command,params)
            else:
              logmsg(logfile,'info','player ping average ('+str(int(avg_ping))+') is within limit ('+str(pinglimit)+')')

            logmsg(logfile,'debug','deleting entries for player in pings db')
            query="DELETE FROM pings WHERE steamid64 = %s"
            values=[]
            values.append(str(player['UniqueId']))
            data=await dbquery(config,query,values)
          else:
            logmsg(logfile,'debug','not enough data on pings yet')

          if str(player['Ping'])=='0': # not sure yet what these are
            logmsg(logfile,'debug','ping is 0 - now set to 12345')
            player['Ping']=12345
          logmsg(logfile,'debug','adding entry for user in pings entry')
          timestamp=datetime.now(timezone.utc)            
          query="INSERT INTO pings ("
          query+="steamid64,ping,timestamp"
          query+=") VALUES (%s,%s,%s)"
          values=[str(player['UniqueId']),player['Ping'],timestamp]
          data=await dbquery(config,query,values)
      case 'pullstats':
        logmsg(logfile,'info','pullstats called')
        command='ServerInfo'
        params={}
        data=await rcon(config,command,params)
        serverinfo=data['ServerInfo']

        matchended=False
        if serverinfo['Teams']==True and serverinfo['GameMode'].upper()=='SND':
          if int(serverinfo['Team0Score'])==10:
            matchended=True
            winningteam='team0'
          if int(serverinfo['Team1Score'])==10:
            matchended=True
            winningteam='team1'

        if matchended==True: # only pull stats if match ended
          logmsg(logfile,'info','end of match detected')
          logmsg(logfile,'info','winning team: '+winningteam)
          if serverinfo['GameMode'].upper()=="SND": # only pull stats in SND
            logmsg(logfile,'info','game is SND')

            if serverinfo['GameMode'].upper()=="SND": # demo rec counts as 1 in snd
              numberofplayers0=serverinfo['PlayerCount'].split('/',2)
              numberofplayers1=numberofplayers0[0]
              numberofplayers2=(int(numberofplayers1)-1)
              maxplayers=numberofplayers0[1]
              numberofplayers=str(numberofplayers2)+'/'+str(maxplayers)
            else:
              numberofplayers=serverinfo['PlayerCount']

            logmsg(logfile,'info','srvname:     '+str(serverinfo['ServerName']))
            logmsg(logfile,'info','playercount: '+str(numberofplayers))
            logmsg(logfile,'info','mapugc:      '+serverinfo['MapLabel'])
            logmsg(logfile,'info','gamemode:    '+serverinfo['GameMode'])
            logmsg(logfile,'info','roundstate:  '+serverinfo['RoundState'])
            logmsg(logfile,'info','teams:       '+str(serverinfo['Teams']))
            if serverinfo['Teams']==True:
              logmsg(logfile,'info','team0score:  '+serverinfo['Team0Score'])
              logmsg(logfile,'info','team1score:  '+serverinfo['Team1Score'])

            command='InspectAll'
            params=[]
            data=await rcon(config,command,params)
            inspectlist=data['InspectList']

            for player in inspectlist:
              logmsg(logfile,'info','player: '+str(player))
              logmsg(logfile,'info','player[PlayerName]: '+str(player['PlayerName']))
              logmsg(logfile,'info','player[UniqueId]: '+str(player['UniqueId']))
              logmsg(logfile,'info','player[KDA]: '+str(player['KDA']))
              kda=player['KDA'].split('/',3)
              kills=kda[0]
              deaths=kda[1]
              average=kda[2]
              score=player['Score']
              ping=player['Ping']
              logmsg(logfile,'info','kills: '+str(kills))
              logmsg(logfile,'info','deaths: '+str(deaths))
              logmsg(logfile,'info','average: '+str(average))
              logmsg(logfile,'info','score: '+str(score))
              logmsg(logfile,'info','ping: '+str(ping))
              if str(player['TeamId'])!='':
                logmsg(logfile,'info','player[TeamId]: '+str(player['TeamId']))

              logmsg(logfile,'info','checking if user exists in db')
              query="SELECT * FROM steamusers WHERE steamid64 = %s LIMIT 1"
              values=[]
              values.append(str(player['UniqueId']))
              data=await dbquery(config,query,values)

              if data['rowcount']==0:
                logmsg(logfile,'info','adding user to db because not found')
                query="INSERT INTO steamusers (steamid64) VALUES (%s)"
                values=[]
                values.append(str(player['UniqueId']))
                data=await dbquery(config,query,values)
              else:
                logmsg(logfile,'info','steam user already in db: '+str(player['UniqueId']))

              logmsg(logfile,'info','getting steamusers id from db (to make sure it exists there)')
              query="SELECT id FROM steamusers WHERE steamid64=%s LIMIT 1"
              values=[]
              values.append(str(player['UniqueId']))
              data=await dbquery(config,query,values)
              steamuserid=data['rows'][0][0]

              logmsg(logfile,'info','adding stats for user')
              timestamp=datetime.now(timezone.utc)            
              query="INSERT INTO stats ("
              query+="steamusers_id,kills,deaths,average,score,ping,servername,playercount,mapugc,gamemode,matchended,teams,team0score,team1score,timestamp"
              query+=") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
              values=[steamuserid,kills,deaths,average,score,ping,serverinfo['ServerName'],str(numberofplayers),serverinfo['MapLabel'],serverinfo['GameMode'],matchended,serverinfo['Teams'],serverinfo['Team0Score'],serverinfo['Team1Score'],timestamp]
              data=await dbquery(config,query,values)
            logmsg(logfile,'info','processed all current players')
          else:
            logmsg(logfile,'info','not pulling stats because not SND')
        else:
          logmsg(logfile,'info','not pulling stats because not match end')
      case _:
        logmsg(logfile,'warn','unknown action - continuing anyway')

  def process_keyword(line,keyword):
    match keyword:
      case 'Rotating map':
        logmsg(logfile,'info','map rotation called')

      case 'LogLoad: LoadMap':
        if '/Game/Maps/ServerIdle' in line:
          logmsg(logfile,'info','map switch called')
        elif '/Game/Maps/download.download' in line:
          mapugc0=line.split('UGC',1)
          mapugc1=('UGC'+mapugc0[1])
          mapugc=mapugc1.strip()
          logmsg(logfile,'info','map is being downloaded: '+mapugc)
        elif 'LoadMap: /UGC' in line:
          mapugc0=line.split('LoadMap: /',1)
          mapugc1=mapugc0[1].split("/",1)
          mapugc=mapugc1[0].strip()
          gamemode0=line.split('game=',1)
          gamemode=gamemode0[1].strip()
          logmsg(logfile,'info','custom map is loading: '+mapugc+' as '+gamemode)
        elif '/Game/Maps' in line:
          mapugc0=line.split('Maps/',1)
          mapugc1=mapugc0[1].split("/",1)
          mapugc=mapugc1[0].strip()
          gamemode0=line.split('game=',1)
          gamemode=gamemode0[1].strip()
          logmsg(logfile,'info','vrankrupt map is loading: '+mapugc+' as '+gamemode)

      case 'Updating blacklist':
        logmsg(logfile,'info','access configs reloaded')

      case 'PavlovLog: StartPlay':
        logmsg(logfile,'info','map started')

      case '"State":':
        roundstate0=line.split('": "',1)
        roundstate1=roundstate0[1].split('"',1)
        roundstate=roundstate1[0].strip()
        logmsg(logfile,'info','round state changed to: '+roundstate)
        match roundstate:
          case 'Starting':
            asyncio.run(action(config,'serverinfo'))
          case 'Started':
            asyncio.run(action(config,'autokickhighping'))
            asyncio.run(action(config,'autopin'))
          case 'StandBy':
            asyncio.run(action(config,'autokickhighping'))
          case 'Ended':
            asyncio.run(action(config,'autokickhighping'))
            asyncio.run(action(config,'pullstats'))

      case 'Preparing to exit':
        logmsg(logfile,'info','server is shutting down')

      case 'LogHAL':
        logmsg(logfile,'info','server is starting up')

      case 'Server Status Helper':
        logmsg(logfile,'info','server is now online')

      case 'Rcon: User':
        rconclient0=line.split(' authenticated ',2)
        rconclient=rconclient0[1].strip()
        logmsg(logfile,'info','rcon client auth: '+rconclient)

      case 'SND: Waiting for players':
        logmsg(logfile,'info','waiting for players')

      case 'long time between ticks':
        logmsg(logfile,'info','long tick detected')

      case 'Login request':
        loginuser0=line.split(' ?Name=',2)
        loginuser1=loginuser0[1].split('?',2)
        loginuser=loginuser1[0]
        loginid0=line.split('NULL:',2)
        loginid1=loginid0[1].split(' ',2)
        loginid=loginid1[0]
        logmsg(logfile,'info','login request from user: '+str(loginuser)+' ('+loginid+')')

      case 'Client netspeed':
        netspeed0=line.split('Client netspeed is ',2)
        netspeed=netspeed0[1]
        logmsg(logfile,'info','client netspeed: '+netspeed.strip())

      case 'Join request':
        joinuser0=line.split('?name=',2)
        joinuser1=joinuser0[1].split('?',2)
        joinuser=joinuser1[0]
        logmsg(logfile,'info','join request from user: '+str(joinuser))

      case 'Join succeeded':
        joinuser0=line.split('succeeded: ',2)
        joinuser=joinuser0[1]
        logmsg(logfile,'info','join successful for user: '+str(joinuser.strip()))

      case 'LogNet: UChannel::Close':
        leaveuser0=line.split('RemoteAddr: ',2)
        leaveuser1=leaveuser0[1].split(',',2)
        leaveuser=leaveuser1[0]
        logmsg(logfile,'info',' user left the server: '+str(leaveuser))

      case '"KillData":':
        logmsg(logfile,'info','a player died...')

      case '"Killer":':
        killer0=line.split('"',4)
        killer=killer0[3]
        logmsg(logfile,'info','killer: '+str(killer))

      case '"KillerTeamID":':
        killerteamid0=line.split('": ',2)
        killerteamid1=killerteamid0[1].split(',',2)
        killerteamid=killerteamid1[0]
        logmsg(logfile,'info','killerteamid: '+killerteamid)

      case '"Killed":':
        killed0=line.split('"',4)
        killed=killed0[3]
        logmsg(logfile,'info','killed: '+str(killed))

      case '"KilledTeamID":':
        killedteamid0=line.split('": ',2)
        killedteamid1=killedteamid0[1].split(',',2)
        killedteamid=killedteamid1[0]
        logmsg(logfile,'info','killedteamid: '+killedteamid)

      case '"KilledBy":':
        killedby0=line.split('"',4)
        killedby=killedby0[3]
        logmsg(logfile,'info','killedby: '+killedby)

      case '"Headshot":':
        headshot0=line.split('": ',2)
        headshot=headshot0[1]
        logmsg(logfile,'info','headhot: '+str(headshot).strip())

      case 'LogTemp: Rcon: KickPlayer':
        kickplayer0=line.split('KickPlayer ',2)
        kickplayer=kickplayer0[1]
        logmsg(logfile,'info','player kicked: '+kickplayer.strip())

      case 'LogTemp: Rcon: BanPlayer':
        banplayer0=line.split('BanPlayer ',2)
        banplayer=banplayer0[1]
        logmsg(logfile,'info','player banned: '+banplayer.strip())

      case 'BombData':
        logmsg(logfile,'info','something happened with the bomb')

      case '"Player":':
        bombplayer0=line.split('": "',2)
        bombplayer1=bombplayer0[1].split('"',2)
        bombplayer=bombplayer1[0]
        logmsg(logfile,'info','player interacted with bomb: '+bombplayer)

      case '"BombInteraction":':
        bombinteraction0=line.split('": "',2)
        bombinteraction1=bombinteraction0[1].split('"',2)
        bombinteraction=bombinteraction1[0]
        logmsg(logfile,'info','bomb interaction: '+ bombinteraction)

  def find_keywords(line,keywords):
   for keyword in keywords:
     if keyword in line:
       logmsg(logfile,'debug','original line: '+line.strip())
       logmsg(logfile,'debug','matched keyword: '+keyword)
       process_keyword(line,keyword)

  def follow_log(target_log):
    seek_end=True
    while True:
      with open(target_log) as f:
        if seek_end:
          f.seek(0,2)
        while True:
          line=f.readline()
          if not line:
            try:
              if f.tell() > os.path.getsize(target_log):
                f.close()
                seek_end = False
                break
            except FileNotFoundError:
              pass
            time.sleep(1)
          yield line

  # say hi
  logmsg(logfile,'info',meta['name']+' '+meta['version']+' has been started')
  logmsg(logfile,'info','target_log given: '+target_log)

  # read the target log, find keywords and do stuff on match
  logmsg(logfile,'info','starting to read from the target log file...')
  loglines=follow_log(target_log)
  for line in loglines:
    if line != "":
      find_keywords(line,keywords)
