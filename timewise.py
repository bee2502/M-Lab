# Script to extract time wise data (here , used for one day with periods of 4 hrs) in a loop for any query
# Sample queries used are defined in project report
# Done by integrating Big Query Command Line Tool with Python

import os

def timewise(year,measure) :
    id='lively-encoder-88714'
    q1="SELECT percentile_cont(0.5) OVER (ORDER BY rtt) FROM ( SELECT web100_log_entry.connection_spec.remote_ip AS ips,AVG(web100_log_entry.snap.SumRTT/web100_log_entry.snap.CountRTT) AS rtt FROM [measurement-lab:m_lab."
    q2="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q3=") / POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q4=") / POW(10, 6) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.MinRTT) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SumRTT) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CountRTT) AND web100_log_entry.snap.CountRTT > 10 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY ips )"

    q5=" SELECT percentile_cont(0.5) OVER (ORDER BY thru) FROM (SELECT web100_log_entry.connection_spec.remote_ip AS ips ,AVG(web100_log_entry.snap.HCThruOctetsAcked/(web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)) AS thru,FROM [measurement-lab:m_lab."
    q6="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q7=") /POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q8=") / POW(10, 6) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd +web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals) AND web100_log_entry.snap.CongSignals > 0 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY ips) "

     
    q9="SELECT percentile_cont(0.5) OVER (ORDER BY retrans) FROM (SELECT web100_log_entry.connection_spec.remote_ip AS ips,AVG(web100_log_entry.snap.SegsRetrans/web100_log_entry.snap.DataSegsOut) AS retrans FROM [measurement-lab:m_lab."
    q10="] WHERE IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND IS_EXPLICITLY_DEFINED(connection_spec.client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q11=") /POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q12=") /POW(10, 6) AND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) AND IS_EXPLICITLY_DEFINED(project) AND project = 0 AND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) AND connection_spec.data_direction = 1 AND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) AND web100_log_entry.is_last_entry = True AND web100_log_entry.snap.HCThruOctetsAcked >= 8192 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000 AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000 AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SegsRetrans) AND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.DataSegsOut) AND web100_log_entry.snap.DataSegsOut > 0 AND (web100_log_entry.snap.State == 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) GROUP BY ips )"



    q13="SELECT COUNT(ips) FROM (SELECT web100_log_entry.connection_spec.remote_ip AS ips FROM [measurement-lab:m_lab."
    q14="] WHERE IS_EXPLICITLY_DEFINED(connection_spec. client_geolocation.country_name) AND connection_spec.client_geolocation.country_name='United States' AND IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time > PARSE_UTC_USEC("
    q15=") / POW(10, 6) AND web100_log_entry.log_time < PARSE_UTC_USEC("
    q16=") / POW(10, 6) GROUP BY ips)"

    time=['00:00:00','04:00:00','08:00:00','12:00:00','16:00:00','20:00:00','23:59:00']
    if measure == 1:
        fname1='timewise1rtt'+str(year)+'.txt'
        print fname1
        #fp1=open(fname,'r+w')
        for i in range(0,6) : 
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-25 "+str(time[i])+"'"
            mend="'"+str(year)+"-12-25 "+str(time[i+1])+"'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : timewise')
        print year , measure
        print('------------------------->')    
    elif measure== 2:
        fname1='timewise1thru'+str(year)+'.txt'
        print fname1
        for i in range(0,6) : 
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-25 "+str(time[i])+"'"
            mend="'"+str(year)+"-12-25 "+str(time[i+1])+"'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : timewise')
        print year , measure
        print('------------------------->')    
    elif measure == 3:
        fname1='timewise1retrans'+str(year)+'.txt'
        print fname1
        for i in range(0,6) : 
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-25 "+str(time[i])+"'"
            mend="'"+str(year)+"-12-25 "+str(time[i+1])+"'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : timewise')
        print year , measure
        print('------------------------->')    
    elif  measure== 4:
        fname1='timewise1numconn'+str(year)+'.txt'
        print fname1
        for i in range(0,6) : 
            mtable=str(year)+"_12" 
            mstart="'"+str(year)+"-12-25 "+str(time[i])+"'"
            mend="'"+str(year)+"-12-25 "+str(time[i+1])+"'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        print('<--------------DONE : timewise')
        print year , measure
        print('------------------------->')    

#timewise(2010,1)
y=[2010,2012,2012]
m=[1,2,3,4]
for i in range(0,3):
    for j in range(0,4):
        print i 
        print j
        timewise(y[i],m[j])
