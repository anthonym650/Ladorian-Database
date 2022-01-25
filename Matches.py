#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from requests_html import HTMLSession
import pandas as pd 
import pandas.io.sql as sqlio
from bs4 import BeautifulSoup
from io import StringIO
import re
import datetime
import psycopg2 as server
import sys
import schedule
import time

#--------------------------------PRELOADED LEAGUES AND TOURNAMENTS--------------------------
league_links_results =  ['https://www.skysports.com/premier-league-results/2019-20',
                            'https://www.skysports.com/premier-league-results/2020-21',
                            'https://www.skysports.com/premier-league-results',
                            'https://www.skysports.com/league-1-results',
                            'https://www.skysports.com/league-1-results/2019-20',
                            'https://www.skysports.com/league-1-results/2020-21',
                            'https://www.skysports.com/la-liga-results/2019-20',
                            'https://www.skysports.com/la-liga-results/2020-21',
                            'https://www.skysports.com/la-liga-results',
                            'https://www.skysports.com/serie-a-results/2019-20',
                            'https://www.skysports.com/serie-a-results/2020-21',
                            'https://www.skysports.com/serie-a-results',
                            'https://www.skysports.com/bundesliga-results/2019-20',
                            'https://www.skysports.com/bundesliga-results/2020-21',
                            'https://www.skysports.com/bundesliga-results',
                            'https://www.skysports.com/champions-league-results/2019-20',
                            'https://www.skysports.com/champions-league-results/2020-21',
                            'https://www.skysports.com/champions-league-results',
                            'https://www.skysports.com/europa-league-results/2019-20',
                            'https://www.skysports.com/europa-league-results/2020-21',
                            'https://www.skysports.com/europa-league-results',
                            'https://www.skysports.com/europa-conference-league-results',
                            'https://www.skysports.com/euro-2020-results',
                            'https://www.skysports.com/world-cup-results',
                            'https://www.skysports.com/uefa-nations-league-results'
                          ]
#---------------------------------FIXTURES----------------------------------------------------------------
league_links_fixtures = ['https://www.skysports.com/premier-league-fixtures',
                        'https://www.skysports.com/la-liga-fixtures',
                        'https://www.skysports.com/ligue-1-fixtures',
                        'https://www.skysports.com/serie-a-fixtures',
                        'https://www.skysports.com/bundesliga-fixtures',
                        'https://www.skysports.com/champions-league-fixtures',
                        'https://www.skysports.com/europa-league-fixtures',
                        'https://www.skysports.com/europa-conference-league-fixtures',
                        'https://www.skysports.com/world-cup-fixtures',
                        'https://www.skysports.com/uefa-nations-league-fixtures'
                        ]
#-----------------------------------------------------------------------------------------------------
leagues_list = ["La Liga","Ligue 1","Serie A", "Bundesliga","Premier League"]

tournaments_list = ["Champions League","Europa League","Europa Conference League","Euro 2020","World Cup","UEFA Nations League"]
#--------------------------------SQL server connection---------------------------------

def new_conn():
    conn = server.connect(host="localhost",database="Ladorian",user="postgres",password="050798")
    return(conn)
#--------------------------------------------------------------------------------------
#-----Helper function to turn Wednesday 12th Jaunary 2020 -> Wednesday 12 January 2020-----
def solve(s):                                             
    return re.sub(r'(\d)(st|nd|rd|th)', r'\1', s)
#--------------------------------------------------------------------------------------
def getTags(link):
    html_file = session.get(link).text
    soup = BeautifulSoup(html_file, features="lxml")
    List_of_tags = [] # A list of h4 and Div tags whose class name is fixres__item#Will look like this: [H4, Div, Div, H4, Div...etc]
 
    #setting the league
    if(soup.find(class_="swap-text__target").string):
        league = soup.find(class_='swap-text__target').string
    else: league = None
    
    body_and_script = [soup.find(class_="fixres__body")]
    
    #adding the additional data after clicking 'show more' on the website
    for script in (soup.find_all('script')): 
        if(script.get('type') == 'text/show-more'):
            script_html = BeautifulSoup(script.string, "html.parser")
            body_and_script.append(script_html)
            
    for j in body_and_script:
        for i in j:
            if(i.name == 'h3'):List_of_tags.append(i)
            if(i.name == "h4"):List_of_tags.append(i)
            if(i.name =='div'):
                if(i["class"][0]=='fixres__item'):
                    List_of_tags.append(i)
                
    return List_of_tags,league

#--------------------------------------------------------------------------------------

def getMatches(temp):
    temp_list=[] # will get wiped out after a new h4 tag is discovered
    List_of_matches = [] #contains elements who are lists of date and their respective matches
    for i in temp:
        if(i.name == 'h3'):
            year = i.string
        elif(i.name == "h4"):
            temp_list=[]
            temp_list.append(solve(i.string+" "+year[-4:]))
            List_of_matches.append(temp_list)

        else: 
            temp_list.append((i.find(class_="matches__item-col matches__participant matches__participant--side1").find(class_="swap-text__target").text,
                              i.find(class_="matches__item-col matches__participant matches__participant--side2").find(class_="swap-text__target").text)
                    )# Finds the home and away teams within fixres__item and adds it to a tuple inside the temp_list
    return List_of_matches
#--------------------------------------------------------------------------------------
def Matches_to_DF(temp):
    f = "%A %d %B %Y" #used in the strptime function to convert Wednesday 12 January 2020 to 2020-01-12
    df = pd.DataFrame(None, columns = ['match_id','tournament_id','date','home_team_id','away_team_id'])
    for i in temp:
        for index, j in enumerate(i):
            if(index != 0):
                df = df.append({'date': datetime.datetime.strptime(i[0],f),'home_team_id':j[0],'away_team_id':j[1]},ignore_index = True)
    return df

#----------------------------------------------------------------------
def update_many(conn, df, table):

    #Fetches table from SQL server into DF
    sql = "SELECT * FROM matches;"
    fetched_data = sqlio.read_sql_query(sql, conn)
    fetched_data.date = pd.to_datetime(fetched_data.date)
    
    #Difference between existing table (DF) and new DF
    df =df.merge(fetched_data, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
    df.drop(columns = ['_merge','match_id'],inplace = True)
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("update_many() done")
    cursor.close()
    conn.close()

#----------------------------------------------------------------------
def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()
    conn.close()
#--------------------------------drops table and creates a new table---------------------------------
def refresh_table(conn):

    cursor = conn.cursor()
    cursor.execute(
    """
    DROP TABLE IF EXISTS matches;
        CREATE TABLE IF NOT EXISTS "matches" (
            match_id SERIAL,
            tournament_id INT,
            date DATE,
            home_team_id INT,
            away_team_id INT,
            league_id INT,
            PRIMARY KEY(match_id),
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
            );
        
    """
    )
    cursor.close()
    conn.commit()

    
def get_table(conn,table):
    """ query parts from the parts table """
    try:
        
        sql ="SELECT *FROM %s" %table    
        fetched_data = sqlio.read_sql_query(sql, conn)
        
        return(fetched_data)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
#-------------------------------------------------------------------------------


session = HTMLSession()
Matches = pd.DataFrame()
for link in league_links_fixtures:
    List_of_tags,league = getTags(link)
    List_of_matches = getMatches(List_of_tags)
    temp_df = Matches_to_DF(List_of_matches)
    if(league in leagues_list):
        temp_df['league_id'] = league
        print(temp_df.league_id[0],"done")
    elif(league in tournaments_list):
        temp_df['tournament_id'] = league
        print(temp_df.tournament_id[0],"done")
    Matches = pd.concat([Matches,temp_df])

teams = get_table(new_conn(),'teams')
tournaments = get_table(new_conn(),'tournaments')
leagues = get_table(new_conn(),'leagues')

FC_r_index =teams.team.loc[teams.team.str.match('.+FC$')== True].index
FC_l_index =teams.team.loc[teams.team.str.match('^FC.+')== True].index
AFC_r_index =teams.team.loc[teams.team.str.match('.+AFC$')== True].index
AFC_l_index =teams.team.loc[teams.team.str.match('^AFC.+')== True].index
teams.loc[teams.team.loc[teams.team.str.match('.+FC$')== True].index,'team'] = teams.loc[teams.team.str.match('.+FC$')== True,'team'].str.rstrip(' FC')
teams.loc[teams.team.loc[teams.team.str.match('^FC.+')== True].index,'team'] = teams.loc[teams.team.str.match('^FC.+')== True,'team'].str.lstrip('FC ')
teams.loc[teams.team.loc[teams.team.str.match('.+AFC$')== True].index,'team'] = teams.loc[teams.team.str.match('.+AFC$')== True,'team'].str.rstrip(' AFC')
teams.loc[teams.team.loc[teams.team.str.match('^AFC.+')== True].index,'team'] = teams.loc[teams.team.str.match('^AFC.+')== True,'team'].str.lstrip('AFC ')

Matches.home_team_id = Matches.home_team_id.replace(teams.set_index('team').to_dict()['team_id'])
Matches.away_team_id = Matches.away_team_id.replace(teams.set_index('team').to_dict()['team_id'])
Matches.tournament_id = Matches.tournament_id.replace(tournaments.set_index('tournament').to_dict()['tournament_id'])
Matches.league_id = Matches.league_id.replace(leagues.set_index('league').to_dict()['league_id'])
Matches.fillna(0,inplace = True)

Matches.reset_index(inplace=True)
Matches.drop(index = Matches.loc[Matches.home_team_id.str.match('[a-zA-Z]+')==True].index,inplace = True)
Matches.drop(index = Matches.loc[Matches.away_team_id.str.match('[a-zA-Z]+')==True].index,inplace = True)

update_many(new_conn(),Matches.loc[:,['tournament_id','date','home_team_id','away_team_id','league_id']],'matches')

