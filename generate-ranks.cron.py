import json
import mysql.connector

if __name__ == '__main__':
    env='live'

    # read config
    config = json.loads(open('config.json').read())[env]

    def dbquery(query,values):
        #print('[DEBUG] dbquery called')
        #print('[DEBUG] query: '+str(query))
        #print('[DEBUG] values: '+str(values))
        #print('[DEBUG] len(values): '+str(len(values)))
        conn=mysql.connector.connect(
            host=config['mysqlhost'],
            port=config['mysqlport'],
            user=config['mysqluser'],
            password=config['mysqlpass'],
            database=config['mysqldatabase'])
        #print('[DEBUG] conn: '+str(conn))
        cursor=conn.cursor(buffered=True,dictionary=True)
        cursor.execute(query,(values))
        conn.commit()
        data={}
        data['rowcount']=cursor.rowcount
        query_type0=query.split(' ',2)
        query_type=str(query_type0[0])
        #print('[DEBUG] query_type: '+query_type)

        if query_type.upper()=="SELECT":
            data['rows']=cursor.fetchall()
            #print('[DEBUG] data[rows]: '+str(data['rows']))
        else:
            data['rows']=False
        cursor.close()
        conn.close()
        #print('[DEBUG] conn and cursor closed')
        return data

    # get all stats for all users
    query="SELECT kills,deaths,average,score,ping"
    query+=",AVG(kills) as avg_kills,AVG(deaths) as avg_deaths,AVG(average) as avg_average,AVG(score) as avg_score,AVG(ping) as avg_ping"
    query+=",MIN(kills) as min_kills,MIN(deaths) as min_deaths,MIN(average) as min_average,MIN(score) as min_score,MIN(ping) as min_ping"
    query+=",MAX(kills) as max_kills,MAX(deaths) as max_deaths,MAX(average) as max_average,MAX(score) as max_score,MAX(ping) as max_ping"
    query+=" FROM stats WHERE gamemode='SND' "
    query+="AND matchended IS TRUE AND playercount=10 "
    query+="ORDER BY timestamp ASC"
    values=[]
    all_stats=dbquery(query,values)
    print('[DEBUG] all_stats: '+str(all_stats))

    # get all steamusers id's from steamusers
    query="SELECT id FROM steamusers"
    values=[]
    steamusers=dbquery(query,values)
    print('[DEBUG] steamusers: '+str(steamusers))
    if steamusers['rowcount']>0:

        for row in steamusers['rows']:
            print('[DEBUG] row: '+str(row))

            steamuser_id=row['id']

            # get stats for current steamuser
            query="SELECT kills,deaths,average,score,ping"
            query+=",AVG(kills) as avg_kills,AVG(deaths) as avg_deaths,AVG(average) as avg_average,AVG(score) as avg_score,AVG(ping) as avg_ping"
            query+=",MIN(kills) as min_kills,MIN(deaths) as min_deaths,MIN(average) as min_average,MIN(score) as min_score,MIN(ping) as min_ping"
            query+=",MAX(kills) as max_kills,MAX(deaths) as max_deaths,MAX(average) as max_average,MAX(score) as max_score,MAX(ping) as max_ping"
            query+=" FROM stats WHERE gamemode='SND' AND steamusers_id=%s "
            query+="AND matchended IS TRUE AND playercount=10 "
            query+="ORDER BY timestamp ASC"
            values=[]
            values.append(steamuser_id)
            player_stats=dbquery(query,values)
            print('[DEBUG] player_stats: '+str(player_stats))

            query="SELECT id FROM stats WHERE gamemode='SND' AND steamusers_id=%s "
            query+="AND matchended IS TRUE AND playercount=10 "
            query+="ORDER BY timestamp ASC"
            values=[]
            values.append(steamuser_id)
            player_all_stats=dbquery(query,values)
            #print('[DEBUG] player_all_stats: '+str(player_all_stats))

            limit_stats=6 # 2 matches with 3 maps
            if player_all_stats['rowcount']>limit_stats:

                player_avg_score=player_stats['rows'][0]['avg_score']
                player_avg_average=player_stats['rows'][0]['avg_average']
                player_avg_kills=player_stats['rows'][0]['avg_kills']
                player_avg_deaths=player_stats['rows'][0]['avg_deaths']
                player_avg_ping=player_stats['rows'][0]['avg_ping']

                player_min_score=player_stats['rows'][0]['min_score']
                player_min_average=player_stats['rows'][0]['min_average']
                player_min_kills=player_stats['rows'][0]['min_kills']
                player_min_deaths=player_stats['rows'][0]['min_deaths']
                player_min_ping=player_stats['rows'][0]['min_ping']

                player_max_score=player_stats['rows'][0]['max_score']
                player_max_average=player_stats['rows'][0]['max_average']
                player_max_kills=player_stats['rows'][0]['max_kills']
                player_max_deaths=player_stats['rows'][0]['max_deaths']
                player_max_ping=player_stats['rows'][0]['max_ping']

                all_avg_score=all_stats['rows'][0]['avg_score']
                all_avg_average=all_stats['rows'][0]['avg_average']
                all_avg_kills=all_stats['rows'][0]['avg_kills']
                all_avg_deaths=all_stats['rows'][0]['avg_deaths']
                all_avg_ping=all_stats['rows'][0]['avg_ping']

                all_min_score=all_stats['rows'][0]['min_score']
                all_min_average=all_stats['rows'][0]['min_average']
                all_min_kills=all_stats['rows'][0]['min_kills']
                all_min_deaths=all_stats['rows'][0]['min_deaths']
                all_min_ping=all_stats['rows'][0]['min_ping']

                all_max_score=all_stats['rows'][0]['max_score']
                all_max_average=all_stats['rows'][0]['max_average']
                all_max_kills=all_stats['rows'][0]['max_kills']
                all_max_deaths=all_stats['rows'][0]['max_deaths']
                all_max_ping=all_stats['rows'][0]['max_ping']

                # prevent divison by 0
                if int(all_max_score)==0: all_max_score=1
                if float(all_max_average)==0: all_max_average=1
                print('[DEBUG] all_max_score: '+str(all_max_score))
                print('[DEBUG] all_max_average: '+str(all_max_average))

                # get relative values (to all spqr players)
                relative_score=10*player_max_score/all_max_score
                relative_average=10*player_max_average/all_max_average
                print('[DEBUG] relative_score: '+str(relative_score))
                print('[DEBUG] relative_average: '+str(relative_average))

                # define weighting between sub-ranks
                score_weighting_factor=0.8
                average_weighting_factor=0.2

                # calc weighted sub-ranks
                weighted_score_rank=relative_score*score_weighting_factor
                weighted_average_rank=relative_average*average_weighting_factor
                print('[DEBUG] weighted_score_rank: '+str(weighted_score_rank))
                print('[DEBUG] weighted_average_rank: '+str(weighted_average_rank))

                # sum up weighted sub-ranks
                sum_weighted_subranks=weighted_score_rank+weighted_average_rank

                # calc final rank
                rank=int(sum_weighted_subranks)

                # get title
                if rank<4: title='Bronze'
                elif rank<7: title='Silver'
                elif rank<10: title='Gold'
                else: title='Platinum'
                
                # check if rank exists for this user
                query="SELECT id FROM ranks WHERE steamusers_id=%s LIMIT 1"
                values=[]
                values.append(steamuser_id)
                existing_rank=dbquery(query,values)

                if existing_rank['rowcount']!=0: # update existing rank
                    print('[DEBUG] updating existing rank in db')
                    query="UPDATE ranks SET "
                    query+="rank=%s,title=%s "
                    query+="WHERE steamusers_id=%s"
                    values=[rank,title,steamuser_id]
                    dbquery(query,values)

                else: # insert new rank
                    print('[DEBUG] inserting new rank to db')
                    query="INSERT INTO ranks ("
                    query+="steamusers_id,rank,title"
                    query+=") VALUES (%s,%s,%s)"
                    values=[steamuser_id,rank,title]
                    dbquery(query,values)

            else:
                print('[DEBUG] not enough stats to generate rank')
    else:
        print('[DEBUG] no steamusers found')