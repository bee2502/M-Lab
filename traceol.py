#!/usr/bin/env python

#This python script runs traceroute experiment on all the websites listed on Alexa 500 list (http://www.alexa.com/topsites) every x minutes
#This script uses online tracing tools(vger.kernel.org is used here) to run the traceroute experiment
#It dumps the traceroute result in timestamped file output_tr_ol.txt for better accesibility of results

#Notes :
#
# I have used BeautifulSoup, a python library for url scraping rather than a regex for accuracy purposes
# Regex will only pick up http/ftp pages or ones in the format I specify. This is not the case with bs4




import urllib
import time 
import datetime, threading
from bs4 import BeautifulSoup
import urllib2
import subprocess
import socket
import shlex



print('Accesing alexa.com 500 global topsites')
pages=[] #stores the urls of 20 pages spanning the whole top500 sites list of www.alexa.com
pages.append("http://www.alexa.com/topsites/global")
for i in range(1,20):
	pages.append("http://www.alexa.com/topsites/global;"+str(i))
#print(pages)




site=[] #stores the 500 sites on which we perform traceroute
print('Scraping alexa.com HTML pages to gather the list of 500 sites')
for i in range(0,20):
	content = urllib2.urlopen(pages[i]).read()
	soup = BeautifulSoup(content)
	links = soup.findAll('a')
	for a in links : 
    		if a.has_attr('href') :   
       			weblinks=a['href']		  
       			ctr1=weblinks.find("siteinfo/")  #formatting for the particular type of links we want
       			if ctr1>0 :
       				site.append(weblinks[ctr1+9:])  
print('alexatop500 sites List collected')      	




print('Writing alexatop500 sites List to alexa500list.txt file')
fp1=open("alexa500list.txt","w")
for i in range(0,len(site)) :
	fp1.write( str(i+1)+"		"+site[i]+"\n")
fp1.close()
print('alexa500list.txt File written')





#This function adds timestamp to a file name
def timeStampFile(filename, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return datetime.datetime.now().strftime(fmt).format(fname=filename)





#This function performs traceroute over the websites in array site( currently contains alexatop500 list) 	
#This function will be executed after every x minutes
def TraceRoute():
	print('\nTraceroute experiment started ')
	print datetime.datetime.now()
	print(" ")
	with open(timeStampFile('output_tr_ol.txt'),'w') as fp2:
		for i in range(0,len(site)):
			hostname=site[i]
			fp2.write("\n"+hostname+"\n")
			print hostname
			testpage= urllib.urlopen("http://vger.kernel.org/cgi-bin/nph-traceroute?ASQ=on&OWN=on&MODE=ICMP&DOMAIN="+str(site[i])) #webpage contais result of traceroute experiment 
			data=testpage.read()	
			start=data.find("<PRE>") #starting index of traceroute data on webpage
			data_endpart=data[start:] #all contents of webpage after start ptr
			end=data_endpart.find("</PRE>") #ending index of traceroute data on remaining webpage(offsetted from original by value=start)
			result=data[start+5:start+end] # end is offsetted
#			print("		index :		"+str(i+1)+"	time :		"+time.strftime("%H:%M")+"		site :		"+str(site[i]))
#			print(result)
#			print("  ")
			fp2.write("		index :		"+str(i+1)+"	time :		"+time.strftime("%H:%M")+"		site :		"+str(site[i]))
			fp2.write(result)
			fp2.write("\n")
	fp2.close()	
	print('\nTraceroute experiment Done for all sites ')
	x=1;
	threading.Timer(60*50, TraceRoute).start() #Runs TraceRoute() after every x minutes

TraceRoute()	

