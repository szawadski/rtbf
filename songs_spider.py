from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, DateTime, NVARCHAR, MetaData
from sqlalchemy.dialects.mysql import insert
import json


url="https://www.rtbf.be/pure/conducteur"

#Getting the webpage, creating a Response object.
response = requests.get(url)

#Extracting the source code of the page.
data = response.text

#Passing the source code to BeautifulSoup to create a BeautifulSoup object for it.
soup = BeautifulSoup(data, 'lxml')

#Extracting data from the soup
tags_name = soup.select('span[itemprop="name"]')
tags_artist = soup.select('span[itemprop="byArtist"]')
tags_time = soup.select('p[class="www-time"]')

#Creating list from strings

#Titles to list
titles=[]
for tag in tags_name:
    titles.append(str(tag.string))
#print(titles)

#Artists to list
artists=[]
for tag in tags_artist:
    artists.append(str(tag.string))
#print(artists)

#Times to list
times=[]
p = re.compile('\d\d:\d\d')
for tag in tags_time:
    times.append(re.findall(p,tag.text)[0])
#print(times)

#Dates to list
dates=[]
p = re.compile('\d\d/\d\d/\d\d\d\d')
for tag in tags_time:
    dates.append(re.findall(p,tag.text)[0])
#print(dates)

#Build datetime object from strings
datetimes=[]
for n in range(len(dates)):
    datetimes.append(datetime.strptime(dates[n]+' '+times[n], '%d/%m/%Y %H:%M'))
#print(datetimes)

#Convert datetime objects to timestamps
timestamps=[]
for dt in datetimes:
    timestamps.append(int(time.mktime(dt.timetuple())))
#print(timestamps)

#Establishing connection to db
engine = create_engine('mysql+mysqlconnector://conducteur:conducteur@192.168.0.103:3307/purefm', echo=False)
conn = engine.connect()

#Define the tables
metadata = MetaData()
conducteur = Table('conducteur', metadata,
                       Column('timestamp',Integer, primary_key=True),
                       Column('datetime', DateTime),
                       Column('artist', NVARCHAR(length=50)),
                       Column('title', NVARCHAR(length=50)))

#Create the tables in db if not exist
metadata.create_all(engine)

#Updating the db
for n in range(len(timestamps)):
    insert_stmt = insert(conducteur).values(
        timestamp=timestamps[n],
        datetime=datetimes[n],
        artist=artists[n],
        title=titles[n])
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            datetime=datetimes[n])
    result = conn.execute(on_duplicate_key_stmt)
    
