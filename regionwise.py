# Script to extract day wise regionwise data (here , used for over a week) in a loop for any query
# Sample queries used are defined in project report
# Done by integrating Big Query Command Line Tool with Python

import os


def daywisereg(year,measure) :
    id='lively-encoder-88714'
    q1="SELECT connection_spec.client_geolocation.region AS regus ,AVG(web100_log_entry.snap.SumRTT/web100_log_entry.snap.CountRTT) AS rtt FROM [measurement-lab:m_lab."
    q2="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q3=") / POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q4=") / POW(10, 6) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.MinRTT) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SumRTT) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CountRTT) AND web100_log_entry.snap.CountRTT > 10 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY regus ORDER BY regus"

    q5="SELECT connection_spec.client_geolocation.region AS regus ,AVG(web100_log_entry.snap.HCThruOctetsAcked/(web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)) AS thru,FROM [measurement-lab:m_lab."
    q6="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q7=") /POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q8=") / POW(10, 6) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd +web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals) AND web100_log_entry.snap.CongSignals > 0 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY regus ORDER BY regus "

     
    q9="SELECT connection_spec.client_geolocation.region AS regus , AVG(web100_log_entry.snap.SegsRetrans/web100_log_entry.snap.DataSegsOut) AS retrans FROM [measurement-lab:m_lab."
    q10="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q11=") /POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q12=") /POW(10, 6) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SegsRetrans) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.DataSegsOut) AND web100_log_entry.snap.DataSegsOut > 0 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY regus ORDER BY regus"



    q13="SELECT SELECT connection_spec.client_geolocation.region AS regus ,COUNT(ips) FROM (SELECT web100_log_entry.connection_spec.remote_ip AS ips FROM [measurement-lab:m_lab."
    q14="] WHERE IS_EXPLICITLY_DEFINED(connection_spec. client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q15=") / POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q16=") / POW(10, 6) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.region) GROUP BY ips) GROUP BY regus ORDER BY regus"
    
    xmas=[22,23,24,25,26,27,28]
    reg=['NH','AR', 'AE','WV', 'IL','UT','NC', 'VA', 'WI', 'CA' , 'AK' , 'NV' , 'NM' , 'IA' , 'ME' , 'CT' , 'MI' , 'ND' , 'NJ' , 'MO' , 'NY' , 'MD', 'VT' , 'DE' , 'CO' , 'AP', 'TN', 'ID', 'HI' , 'FL' , 'OR' , 'KS' , 'DC' , 'AL' , 'IN' , 'SD' ,'KY', 'AZ' ,'MS' ,'PA', 'RI', 'MN' ,'MT', 'MA', 'LA' ,'WY' , 'TX' ,'SC' , 'NE' ,'OK' ,'OH' ,'GA' ,'WA' ]
    
    if measure == 1:
        #fp1=open(fname,'r+w')
        for i in range(0,len(xmas)) : 
            fname1='daywisereg'+str(i+1)+'rtt'+str(year)+'.txt'
            print fname1
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-"+str(xmas[i])+" 00:00:00'"
            mend="'"+str(year)+"-12-"+str(xmas[i])+" 23:59:00'"
            mreg=str(reg[j])
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=100 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : daywisereg')
        print year , measure
        print('------------------------->')    
    elif measure== 2:
        for i in range(0,len(xmas)) : 
            fname1='daywisereg'+str(i+1)+'thru'+str(year)+'.txt'
            print fname1
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-"+str(xmas[i])+" 00:00:00'"
            mend="'"+str(year)+"-12-"+str(xmas[i])+" 23:59:00'"
            mreg=str(reg[j])
            q=str(q5)+str(mtable)+str(q6)+str(mstart)+str(q7)+str(mend)+str(q8)
            cmdq='bq --project_id='+str(id)+' query --max_rows=100 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : daywisereg')
        print year , measure
        print('------------------------->')    
    elif measure == 3:
        for i in range(0,len(xmas)) : 
            fname1='daywisereg'+str(i+1)+'retrans'+str(year)+'.txt'
            print fname1
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-"+str(xmas[i])+" 00:00:00'"
            mend="'"+str(year)+"-12-"+str(xmas[i])+" 23:59:00'"
            mreg=str(reg[j])
            q=str(q9)+str(mtable)+str(q10)+str(mstart)+str(q11)+str(mend)+str(q12)
            cmdq='bq --project_id='+str(id)+' query --max_rows=100 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : daywisereg')
        print year , measure
        print('------------------------->')    
    elif  measure== 4:
        for i in range(0,len(xmas)) : 
            fname1='daywisereg'+str(i+1)+'numconn'+str(year)+'.txt'
            print fname1
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-"+str(xmas[i])+" 00:00:00'"
            mend="'"+str(year)+"-12-"+str(xmas[i])+" 23:59:00'"
            mreg=str(reg[j])
            q=str(q13)+str(mtable)+str(q14)+str(mstart)+str(q15)+str(mend)+str(q16)
            cmdq='bq --project_id='+str(id)+' query --max_rows=100 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : daywisereg')
        print year , measure
        print('------------------------->')    

#daywisereg(2010,1)
y=[2010,2011,2012]
m=[1,2,3,4]
for i in range(0,3):
    for j in range(0,4):
        print i 
        print j
        daywisereg(y[i],m[j])
