# Script to get isp from ip using ipinfo.io api
#ip imported from iplist.txt file 
#corresponding isp exported to isplist.txt file
# Sample queries used are defined in project report


import os

with open("iplist.txt") as fp1:
    for line in fp1:
    	line=line.rstrip('\n')
       	ip=str(line)
       	cmdl="curl ipinfo.io/"+str(ip)+"/org >> isplist.txt"
       	print cmdl   
       	os.system(cmdl)         
