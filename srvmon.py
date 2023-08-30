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
    if bool(config['debug'])==True:
        level=logging.DEBUG
    else:
        level=logging.INFO
    logging.basicConfig(
        filename='spqr-pavlov-srvmon.log',
        filemode='a',
        format='%(asctime)s,%(msecs)d [%(levelname)s] srvmon: %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S',
        level=level)
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

    # query database
    def dbquery(query,values):
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

    # use pavrlovcon
    async def rcon(rconcmd,rconparams):
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

    # get serverinfo via rcon
    async def get_serverinfo():
        logmsg(logfile,'info','get_serverinfo called')
        data=await rcon('ServerInfo',{})
        serverinfo=data['ServerInfo']

        # make sure gamemode is uppercase
        serverinfo['GameMode']=serverinfo['GameMode'].upper()

        # demo rec counts as 1 player in SND
        if serverinfo['GameMode']=="SND":
            numberofplayers0=serverinfo['PlayerCount'].split('/',2)
            numberofplayers1=numberofplayers0[0]
            numberofplayers2=(int(numberofplayers1)-1)
            maxplayers=numberofplayers0[1]
            numberofplayers=str(numberofplayers2)+'/'+str(maxplayers)
        else:
            numberofplayers=serverinfo['PlayerCount']
        serverinfo['PlayerCount']=numberofplayers

        # for SND get info if match has ended and which team won if there were teams
        serverinfo['MatchEnded']=False
        if serverinfo['GameMode']=="SND" and serverinfo['Teams'] is True:
            if int(serverinfo['Team0Score'])==10:
                serverinfo['MatchEnded']=True
                serverinfo['WinningTeam']='team0'
            if int(serverinfo['Team1Score'])==10:
                serverinfo['MatchEnded']=True
                serverinfo['WinningTeam']='team1'
        if serverinfo['MatchEnded'] is True:
            logmsg(logfile,'info','end of match detected')
            logmsg(logfile,'info','winning team: '+str(serverinfo['WinningTeam']))

        return serverinfo

    # retrieve and output serverinfo
    async def action_serverinfo():
        logmsg(logfile,'info','action_serverinfo called')
        serverinfo=await get_serverinfo()
        logmsg(logfile,'info','srvname:     '+str(serverinfo['ServerName']))
        logmsg(logfile,'info','playercount: '+str(serverinfo['PlayerCount']))
        logmsg(logfile,'info','mapugc:      '+str(serverinfo['MapLabel']))
        logmsg(logfile,'info','gamemode:    '+str(serverinfo['GameMode']))
        logmsg(logfile,'info','roundstate:  '+str(serverinfo['RoundState']))
        logmsg(logfile,'info','teams:       '+str(serverinfo['Teams']))
        if serverinfo['Teams']==True:
            logmsg(logfile,'info','team0score:  '+str(serverinfo['Team0Score']))
            logmsg(logfile,'info','team1score:  '+str(serverinfo['Team1Score']))

    # set pin depending on map, playercount and gamemode
    async def action_autopin():
        logmsg(logfile,'info','action_autopin called')
        serverinfo=await get_serverinfo()

        limit=10
        if serverinfo['GameMode']=="TDM":
            if serverinfo['MapLabel']=="UGC2814848": # aimmap
                limit=8
        elif serverinfo['GameMode']=="DM":
            if serverinfo['MapLabel']=="UGC3037601": # poolday
                limit=5
        playercount_split=serverinfo['PlayerCount'].split('/',2)
        if (int(playercount_split[0]))>=limit:
            logmsg(logfile,'info','limit ('+str(limit)+') reached - setting pin 9678')
            command='SetPin'
            params={'9678'}
            data=await rcon(command,params)
        else:
            logmsg(logfile,'info','below limit ('+str(limit)+') - removing pin')
            command='SetPin'
            params={''}
            data=await rcon(command,params)

    # kick players with high pings
    async def action_autokickhighping():
        logmsg(logfile,'info','action_autokickhighping called')
        data=await rcon('InspectAll',{})
        inspectlist=data['InspectList']

        for player in inspectlist:
            logmsg(logfile,'debug','searching for other entries for this player in pings db')

            query="SELECT steamid64,ping,"
            query+="AVG(ping) as avg_ping,"
            query+="MIN(ping) as min_ping,"
            query+="MAX(ping) as max_ping,"
            query+="COUNT(id) as cnt_id "
            query+="FROM pings "
            query+="WHERE steamid64 = %s"
            values=[]
            values.append(str(player['UniqueId']))
            data=dbquery(query,values)

            avg_ping=data['rows'][0][2]
            min_ping=data['rows'][0][3]
            max_ping=data['rows'][0][4]
            cnt_ping=data['rows'][0][5]
            logmsg(logfile,'debug','found '+str(cnt_ping)+' entries for player: '+str(player['UniqueId']))
            logmsg(logfile,'debug','avg_ping: '+str(avg_ping))
            logmsg(logfile,'debug','min_ping: '+str(min_ping))
            logmsg(logfile,'debug','max_ping: '+str(max_ping))
            minentries=5
            if cnt_ping>=minentries: # dont do anything, unless there are >=minentries for a player
                logmsg(logfile,'debug','rowcount ('+str(cnt_ping)+') >= minentries ('+str(minentries)+')')
                pinglimit=59
                logmsg(logfile,'debug','checking wether limit has been reached or not for player: '+str(player['UniqueId']))
                if int(avg_ping)>pinglimit:
                    logmsg(logfile,'info','player ping average ('+str(int(avg_ping))+') exceeds the limit ('+str(pinglimit)+')')
                    await rcon('Kick',{str(player['UniqueId'])})
                else:
                    logmsg(logfile,'info','player ping average ('+str(int(avg_ping))+') is within limit ('+str(pinglimit)+')')

                logmsg(logfile,'debug','deleting entries for player in pings db')
                query="DELETE FROM pings WHERE steamid64 = %s"
                values=[]
                values.append(str(player['UniqueId']))
                dbquery(query,values)
            else:
                logmsg(logfile,'debug','not enough data on pings yet')

            if str(player['Ping'])=='0': # not sure yet what these are
                logmsg(logfile,'debug','ping is 0 - now set to 12345 for debugging purposes')
                player['Ping']=1234
            logmsg(logfile,'debug','adding entry for user in pings entry')
            timestamp=datetime.now(timezone.utc)            
            query="INSERT INTO pings ("
            query+="steamid64,ping,timestamp"
            query+=") VALUES (%s,%s,%s)"
            values=[str(player['UniqueId']),player['Ping'],timestamp]
            dbquery(query,values)

    # pull stats
    async def action_pullstats():
        logmsg(logfile,'info','pullstats called')
        serverinfo=await get_serverinfo()

        #if serverinfo['MatchEnded'] is True and serverinfo['GameMode']=="SND": # only pull stats if match ended and only in SND
        if True is True: # for debugging
            logmsg(logfile,'info','actually pulling stats now')
            data=await rcon('InspectAll',{})
            inspectlist=data['InspectList']
            for player in inspectlist:
                kda=player['KDA'].split('/',3)
                kills=kda[0]
                deaths=kda[1]
                average=kda[2]
                score=player['Score']
                ping=player['Ping']

                logmsg(logfile,'debug','player: '+str(player))
                logmsg(logfile,'debug','player[PlayerName]: '+str(player['PlayerName']))
                logmsg(logfile,'debug','player[UniqueId]: '+str(player['UniqueId']))
                logmsg(logfile,'debug','player[KDA]: '+str(player['KDA']))
                logmsg(logfile,'debug','kills: '+str(kills))
                logmsg(logfile,'debug','deaths: '+str(deaths))
                logmsg(logfile,'debug','average: '+str(average))
                logmsg(logfile,'debug','score: '+str(score))
                logmsg(logfile,'debug','ping: '+str(ping))
                if str(player['TeamId'])!='':
                    logmsg(logfile,'info','player[TeamId]: '+str(player['TeamId']))

                logmsg(logfile,'info','checking if user exists in db')
                query="SELECT * FROM steamusers WHERE steamid64 = %s LIMIT 1"
                values=[]
                values.append(str(player['UniqueId']))
                data=dbquery(query,values)

                if data['rowcount']==0:
                    logmsg(logfile,'info','adding user to db because not found')
                    query="INSERT INTO steamusers (steamid64) VALUES (%s)"
                    values=[]
                    values.append(str(player['UniqueId']))
                    data=dbquery(query,values)
                else:
                    logmsg(logfile,'info','steam user already in db: '+str(player['UniqueId']))

                logmsg(logfile,'info','getting steamusers id from db (to make sure it exists there)')
                query="SELECT id FROM steamusers WHERE steamid64=%s LIMIT 1"
                values=[]
                values.append(str(player['UniqueId']))
                data=dbquery(query,values)
                steamuserid=data['rows'][0][0]

                logmsg(logfile,'info','adding stats for user')
                timestamp=datetime.now(timezone.utc)            
                query="INSERT INTO stats ("
                query+="steamusers_id,kills,deaths,average,score,ping,servername,playercount,mapugc,"
                query+="gamemode,matchended,teams,team0score,team1score,timestamp"
                query+=") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                values=[
                    steamuserid,kills,deaths,average,score,ping,serverinfo['ServerName'],serverinfo['PlayerCount'],
                    serverinfo['MapLabel'],serverinfo['GameMode'],serverinfo['MatchEnded'],serverinfo['Teams'],
                    serverinfo['Team0Score'],serverinfo['Team1Score'],timestamp]
                data=dbquery(query,values)
            logmsg(logfile,'info','processed all current players')
        else:
            logmsg(logfile,'info','not pulling stats')

    # decide what to do once a keyword appears
    def process_keyword(line,keyword):
        match keyword:
            case 'Rotating map':
                logmsg(logfile,'info','map rotation called')

            case 'LogLoad: LoadMap':
                if '/Game/Maps/ServerIdle' in line:
                    logmsg(logfile,'info','map switch called')
                elif '/Game/Maps/download.download' in line:
                    mapugc0=line.split('UGC',1)
                    mapugc=('UGC'+str(mapugc0[1]))
                    logmsg(logfile,'info','map is being downloaded: '+str(mapugc).strip())
                elif 'LoadMap: /UGC' in line:
                    mapugc0=line.split('LoadMap: /',1)
                    mapugc1=mapugc0[1].split("/",1)
                    mapugc=mapugc1[0]
                    gamemode0=line.split('game=',1)
                    gamemode=gamemode0[1]
                    logmsg(logfile,'info','custom map is loading: '+str(mapugc).strip()+' as '+str(gamemode).strip())
                elif '/Game/Maps' in line:
                    mapugc0=line.split('Maps/',1)
                    mapugc1=mapugc0[1].split("/",1)
                    mapugc=mapugc1[0]
                    gamemode0=line.split('game=',1)
                    gamemode=gamemode0[1]
                    logmsg(logfile,'info','vrankrupt map is loading: '+str(mapugc).strip()+' as '+str(gamemode).strip())

            case 'Updating blacklist':
                logmsg(logfile,'info','access configs reloaded')

            case 'PavlovLog: StartPlay':
                logmsg(logfile,'info','map started')

            case '"State":':
                roundstate0=line.split('": "',1)
                roundstate1=roundstate0[1].split('"',1)
                roundstate=roundstate1[0]
                logmsg(logfile,'info','round state changed to: '+str(roundstate).strip())
                match roundstate:
                    case 'Starting':
                        asyncio.run(action_serverinfo())
                    case 'Started':
                        asyncio.run(action_autokickhighping())
                        asyncio.run(action_autopin())
                    case 'StandBy':
                        asyncio.run(action_autokickhighping())
                    case 'Ended':
                        asyncio.run(action_autokickhighping())
                        asyncio.run(action_pullstats())

            case 'Preparing to exit':
                logmsg(logfile,'info','server is shutting down')

            case 'LogHAL':
                logmsg(logfile,'info','server is starting up')

            case 'Server Status Helper':
                logmsg(logfile,'info','server is now online')

            case 'Rcon: User':
                rconclient0=line.split(' authenticated ',2)
                if len(rconclient0)>1:
                    rconclient=rconclient0[1]
                else:
                    rconclient=rconclient0[0]
                logmsg(logfile,'info','rcon client auth: '+str(rconclient).strip())

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
                logmsg(logfile,'info','login request from user: '+str(loginuser).strip()+' ('+str(loginid).strip()+')')

            case 'Client netspeed':
                netspeed0=line.split('Client netspeed is ',2)
                netspeed=netspeed0[1]
                logmsg(logfile,'info','client netspeed: '+str(netspeed).strip())

            case 'Join request':
                joinuser0=line.split('?name=',2)
                joinuser1=joinuser0[1].split('?',2)
                joinuser=joinuser1[0]
                logmsg(logfile,'info','join request from user: '+str(joinuser).strip())

            case 'Join succeeded':
                joinuser0=line.split('succeeded: ',2)
                joinuser=joinuser0[1]
                logmsg(logfile,'info','join successful for user: '+str(joinuser).strip())

            case 'LogNet: UChannel::Close':
                leaveuser0=line.split('RemoteAddr: ',2)
                leaveuser1=leaveuser0[1].split(',',2)
                leaveuser=leaveuser1[0]
                logmsg(logfile,'info',' user left the server: '+str(leaveuser).strip())

            case '"KillData":':
                logmsg(logfile,'info','a player died...')
                asyncio.run(action_autokickhighping())

            case '"Killer":':
                killer0=line.split('"',4)
                killer=killer0[3]
                logmsg(logfile,'info','killer: '+str(killer).strip())

            case '"KillerTeamID":':
                killerteamid0=line.split('": ',2)
                killerteamid1=killerteamid0[1].split(',',2)
                killerteamid=killerteamid1[0]
                logmsg(logfile,'info','killerteamid: '+str(killerteamid).strip())

            case '"Killed":':
                killed0=line.split('"',4)
                killed=killed0[3]
                logmsg(logfile,'info','killed: '+str(killed).strip())

            case '"KilledTeamID":':
                killedteamid0=line.split('": ',2)
                killedteamid1=killedteamid0[1].split(',',2)
                killedteamid=killedteamid1[0]
                logmsg(logfile,'info','killedteamid: '+str(killedteamid).strip())

            case '"KilledBy":':
                killedby0=line.split('"',4)
                killedby=killedby0[3]
                logmsg(logfile,'info','killedby: '+str(killedby).strip())

            case '"Headshot":':
                headshot0=line.split('": ',2)
                headshot=headshot0[1]
                logmsg(logfile,'info','headhot: '+str(headshot).strip())

            case 'LogTemp: Rcon: KickPlayer':
                kickplayer0=line.split('KickPlayer ',2)
                kickplayer=kickplayer0[1]
                logmsg(logfile,'info','player kicked: '+str(kickplayer).strip())

            case 'LogTemp: Rcon: BanPlayer':
                banplayer0=line.split('BanPlayer ',2)
                banplayer=banplayer0[1]
                logmsg(logfile,'info','player banned: '+str(banplayer).strip())

            case 'BombData':
                logmsg(logfile,'info','something happened with the bomb')
                asyncio.run(action_autokickhighping())

            case '"Player":':
                bombplayer0=line.split('": "',2)
                bombplayer1=bombplayer0[1].split('"',2)
                bombplayer=bombplayer1[0]
                logmsg(logfile,'info','player interacted with bomb: '+str(bombplayer).strip())

            case '"BombInteraction":':
                bombinteraction0=line.split('": "',2)
                bombinteraction1=bombinteraction0[1].split('"',2)
                bombinteraction=bombinteraction1[0]
                logmsg(logfile,'info','bomb interaction: '+ str(bombinteraction).strip())

    # find relevant keywords in target log
    def find_keywords(line,keywords):
        for keyword in keywords:
            if keyword in line:
                logmsg(logfile,'debug','original line: '+str(line).strip())
                logmsg(logfile,'debug','matched keyword: '+str(keyword).strip())
                process_keyword(line,keyword)

    # continously read from the target log
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
    logmsg(logfile,'info','target_log given: '+config['logfile_path'])

    # read the target log, find keywords and do stuff on match
    logmsg(logfile,'info','starting to read from the target log file...')
    loglines=follow_log(config['logfile_path'])
    for line in loglines:
        if line != "":
            find_keywords(line,keywords)
