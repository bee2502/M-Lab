# Script to extract day wise data (here , used for over a week) in a loop for any query
# Sample queries used are defined in project report
# Done by integrating Big Query Command Line Tool with Python

import os

def daywise(year,measure) :
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

    thanksgiving=[19,20,21,22,23,24,25,26,27]
    indep=[1,2,3,4,5,6,7]

    if measure == 1:
        fname1='daywise1rtt'+str(year)+'.txt'
        print fname1
        #fp1=open(fname,'r+w')
        for i in range(0,9) : 
            mtable=str(year)+"_11" 
            mstart="'"+str(year)+"-11-"+str(thanksgiving[i])+" 00:00:00'"
            mend="'"+str(year)+"-11-"+str(thanksgiving[i])+" 23:59:00'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        fname2='daywise2rtt'+str(year)+'.txt'
        print fname2
        #fp1=open(fname,'r+w')
        for i in range(0,7) : 
            mtable=str(year)+"_07" 
            mstart="'"+str(year)+"-07-"+str(indep[i])+" 00:00:00'"
            mend="'"+str(year)+"-07-"+str(indep[i])+" 23:59:00'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname2)
            print i,cmdq
            os.system(cmdq)    
        print('<--------------DONE : daywise')
        print year , measure
        print('------------------------->')    
    elif measure== 2:
        fname1='daywise1thru'+str(year)+'.txt'
        print fname1
        #fp1=open(fname,'r+w')
        for i in range(0,9) : 
            mtable=str(year)+"_11" 
            mstart="'"+str(year)+"-11-"+str(thanksgiving[i])+" 00:00:00'"
            mend="'"+str(year)+"-11-"+str(thanksgiving[i])+" 23:59:00'"
            q=str(q4)+str(mtable)+str(q5)+str(mstart)+str(q6)+str(mend)+str(q7)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        fname2='daywise2thru'+str(year)+'.txt'
        print fname2
        #fp1=open(fname,'r+w')
        for i in range(0,7) : 
            mtable=str(year)+"_07" 
            mstart="'"+str(year)+"-07-"+str(indep[i])+" 00:00:00'"
            mend="'"+str(year)+"-07-"+str(indep[i])+" 23:59:00'"
            q=str(q4)+str(mtable)+str(q5)+str(mstart)+str(q6)+str(mend)+str(q7)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname2)
            print i,cmdq
            os.system(cmdq)    
        print('<--------------DONE : daywise')
        print year , measure
        print('------------------------->')    
    elif measure == 3:
        fname1='daywise1retrans'+str(year)+'.txt'
        print fname1
        #fp1=open(fname,'r+w')
        for i in range(0,9) : 
            mtable=str(year)+"_11" 
            mstart="'"+str(year)+"-11-"+str(thanksgiving[i])+" 00:00:00'"
            mend="'"+str(year)+"-11-"+str(thanksgiving[i])+" 23:59:00'"
            q=str(q8)+str(mtable)+str(q9)+str(mstart)+str(q10)+str(mend)+str(q11)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        fname2='daywise2retrans'+str(year)+'.txt'
        print fname2
        #fp1=open(fname,'r+w')
        for i in range(0,7) : 
            mtable=str(year)+"_07" 
            mstart="'"+str(year)+"-07-"+str(indep[i])+" 00:00:00'"
            mend="'"+str(year)+"-07-"+str(indep[i])+" 23:59:00'"
            q=str(q8)+str(mtable)+str(q9)+str(mstart)+str(q10)+str(mend)+str(q11)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname2)
            print i,cmdq
            os.system(cmdq)    
        print('<--------------DONE : daywise')
        print year , measure
        print('------------------------->')    
    elif  measure== 4:
        fname1='daywise1numconn'+str(year)+'.txt'
        print fname1
        #fp1=open(fname,'r+w')
        for i in range(0,9) : 
            mtable=str(year)+"_11" 
            mstart="'"+str(year)+"-11-"+str(thanksgiving[i])+" 00:00:00'"
            mend="'"+str(year)+"-11-"+str(thanksgiving[i])+" 23:59:00'"
            q=str(q12)+str(mtable)+str(q13)+str(mstart)+str(q14)+str(mend)+str(q15)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname1)
            print i,cmdq
            os.system(cmdq)
        fname2='daywise2numconn'+str(year)+'.txt'
        print fname2
        #fp1=open(fname,'r+w')
        for i in range(0,7) : 
            mtable=str(year)+"_07" 
            mstart="'"+str(year)+"-07-"+str(indep[i])+" 00:00:00'"
            mend="'"+str(year)+"-07-"+str(indep[i])+" 23:59:00'"
            q=str(q12)+str(mtable)+str(q13)+str(mstart)+str(q14)+str(mend)+str(q15)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname2)
            print i,cmdq
            os.system(cmdq)    
        print('<--------------DONE : daywise')
        print year , measure
        print('------------------------->')    

#daywise(2010,1)
y=[2010,2011,2012]
m=[1,2,3,4]
for i in range(0,3):
    for j in range(0,4):
        print i 
        print j
        daywise(y[i],m[j])
