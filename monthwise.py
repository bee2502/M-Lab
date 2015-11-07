# Script to extract month wise yearly data in a loop for any query
# Sample queries used are defined in project report
# Done by integrating Big Query Command Line Tool with Python
# THIS IS WORK OF MS.Bhagyashree Uday Padalkar as a part of OUtreachy 2015 Application

import os

def monthly(year,measure) :
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

    if measure == 1:
        fname='monthlyrtt'+str(year)+'.txt'
        print fname
        #fp1=open(fname,'r+w')
        for i in range(1,10) : 
            mtable=str(year)+"_0"+str(i)   
            mstart="'"+str(year)+"-"+str(0)+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(0)+str(i)+"-31 00:00:00'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
        for i in range(10,13) : 
            mtable=str(year)+"_"+ str(i) 
            mstart="'"+str(year)+"-"+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(i)+"-31 00:00:00'"
            q=str(q1)+str(mtable)+str(q2)+str(mstart)+str(q3)+str(mend)+str(q4)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
        #fp1.close()    
        print('<--------------DONE : monthly')
        print year , measure
        print('------------------------->')    
    elif measure== 2:
        fname='monthlythru'+str(year)+'.txt'
#        fp2=open(fname,'r+w')
        for i in range(1,10) : 
            mtable=str(year)+"_0"+str(i)   
            mstart="'"+str(year)+"-"+str(0)+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(0)+str(i)+"-31 00:00:00'"
            q=str(q5)+str(mtable)+str(q6)+str(mstart)+str(q7)+str(mend)+str(q8)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
        for i in range(10,13) : 
            mtable=str(year)+"_"+ str(i) 
            mstart="'"+str(year)+"-"+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(i)+"-31 00:00:00'"
            q=str(q5)+str(mtable)+str(q6)+str(mstart)+str(q7)+str(mend)+str(q8)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
 #       fp2.close()    
        print('<--------------DONE : monthly')
        print year , measure
        print('------------------------->')    
    elif measure == 3:
        fname='monthlyretrans'+str(year)+'.txt'
#        fp3=open(fname,'r+w')
        for i in range(1,10) : 
            mtable=str(year)+"_0"+str(i)   
            mstart="'"+str(year)+"-"+str(0)+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(0)+str(i)+"-31 00:00:00'"
            q=str(q9)+str(mtable)+str(q10)+str(mstart)+str(q11)+str(mend)+str(q12)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
        for i in range(10,13) : 
            mtable=str(year)+"_"+ str(i) 
            mstart="'"+str(year)+"-"+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(i)+"-31 00:00:00'"
            q=str(q9)+str(mtable)+str(q10)+str(mstart)+str(q11)+str(mend)+str(q12)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
#        fp3.close()    
        print('<--------------DONE : monthly')
        print year , measure
        print('------------------------->')    
    elif  measure== 4:
        fname='monthlynumconn'+str(year)+'.txt'
 #       fp4=open(fname,'r+w')
        for i in range(1,10) : 
            mtable=str(year)+"_0"+str(i)   
            mstart="'"+str(year)+"-"+str(0)+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(0)+str(i)+"-31 00:00:00'"
            q=str(q13)+str(mtable)+str(q14)+str(mstart)+str(q15)+str(mend)+str(q16)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
        for i in range(10,13) : 
            mtable=str(year)+"_"+ str(i) 
            mstart="'"+str(year)+"-"+str(i)+"-01 00:00:00'"
            mend="'"+str(year)+"-"+str(i)+"-31 00:00:00'"
            q=str(q13)+str(mtable)+str(q14)+str(mstart)+str(q15)+str(mend)+str(q16)
            cmdq='bq --project_id='+str(id)+' query --max_rows=1 "'+str(q)+'" >> '+str(fname)
            print i,cmdq
            os.system(cmdq)
#        fp4.close()    
        print('<--------------DONE : monthly')
        print year , measure
        print('------------------------->')    

#monthly(2010,1)
y=[2010,2011,2012]
m=[1,2,3,4]
for i in range(2,3):
    for j in range(2,4):
        print i 
        print j
        monthly(y[i],m[j])