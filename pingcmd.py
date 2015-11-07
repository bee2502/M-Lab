#!/usr/bin/env python

#This python script runs ping experiment on all the websites listed on Alexa 500 list (http://www.alexa.com/topsites) every x minutes
# This script uses ping shell command to run ping experiment
# Command format : ping -c ping_no domain -c ping_no name /ping ip   
# Command example : ping -c 3 www.google.com / ping -c 3 127.0.0.1
# Here -c ping_no ensure the script pings only ping_no times at max
#It dumps the ping result in timestamped file output_ping.txt for better accesibility of results

#Notes :
#
# I have used bs4, a python library for url scraping rather than a regex for accuracy purposes
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




#This function performs ping over the websites in array site( currently contains alexatop500 list) 	
#This function will be executed after every x minutes
def PingExp():
	print('\nPing experiment started ')
	print datetime.datetime.now()
	print(" ")
	with open(timeStampFile('output_ping.txt'),'w') as fp2:
		for i in range(0,len(site)):
			hostname=site[i]
			fp2.write("\n"+hostname+"\n")
			print hostname
			addr=socket.gethostbyname(hostname)  #gets ip of the host : Not all systems can handle domain names as command line arg hence ip used 
			addr='10.16.0.254' #This is the server I tested it with			
			command_line = "ping -c 3 "+str(addr)
  			args = shlex.split(command_line)  
			#executes command_line(which has ping command currently) and stores op in pipe line by line
			ping= subprocess.Popen(args,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in iter(ping.stdout.readline,""):
	    	#		print(line)
    			fp2.write(line+"\n")
			ping.wait()
	fp2.close()  
	print('\nPing experiment Done for all sites ')
	x=1;
	threading.Timer(60*x, PingExp).start() #Runs PingExp() after every x minutes 



PingExp()	
















