#imports
from urllib2 import urlopen 
import pandas as pd

#get data and return dataframe.  Some date parsing, but no time parsing
def getSched(team=133):
	#capture URL for given team
	url ="http://mlb.mlb.com/soa/ical/schedule.csv?home_team_id=" +str(team) +"&season=2015"
	response = urlopen(url)
	data = pd.read_csv(response, parse_dates=['START_DATE'])
	keep = ['START_DATE', 'START_TIME', 
        'END_DATE', 'END_TIME',
        'SUBJECT', 'LOCATION']
	data = data[keep]
	print data.head(10)
	return data

if __name__ == "__main__":
	giants = 137
	oak = 133

	print "getting Giants"
	giantSched=getSched(giants)
	

	print "getting Oakland"
	oakSched = getSched(oak)
	
