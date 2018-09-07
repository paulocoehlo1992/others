# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 23:00:29 2017

@author: Vishal_digit
"""
from flask import Flask,jsonify,request,g
from logging.handlers import RotatingFileHandler
from time import strftime
import traceback
import logging #importing required modules
app=Flask(__name__)
import pandas as pd 
import sqlalchemy
import fuzzywuzzy
from fuzzywuzzy import process as p
from operator import itemgetter
#from fuzzywuzzy import process
import numpy as np 
import os
import time
from functools import reduce

s="select mk.make, md.model, vt.variant, vt.vehicle_code, vt.fuel_type,vt.cubic_capacity, vt.horse_power_bhp, vt.vehicle_category, vt.is_declined, vt.declined_reason, vt.gross_vehicle_weight, vt.seating_capacity, vt.body_type from digit_motor.t_make_master mk join digit_motor.t_model_master md on mk.make_id = md.make_id join digit_motor.t_variant_master vt on md.model_id = vt.model_id"
#engine=sqlalchemy.create_engine("postgresql://readonly:read_only@godigitdev-db01.cpmhro02yml1.ap-south-1.rds.amazonaws.com/digit_dev")
db_string="postgresql://digit_analytics:@nAlY_D1giT@abs-prod-replica.c0rtobvhsnsa.ap-south-1.rds.amazonaws.com/digit_prod"
engine=sqlalchemy.create_engine(db_string)
engine.connect()
df=pd.read_sql(s,con=engine)
master=df[df['vehicle_category']==11]

#cwd=os.getcwd()
#os.chdir(r"C:\Users\vishal\fuzzy_service\fuzzywuzzy_data")
#master=pd.read_csv("master_vehicle.csv",low_memory=False,encoding='iso-8859-1')
master.columns=['Make Name', 'Model Name', 'Variant Name','VEHICLE CODE','Fuel Type','Cubic Capacity','BHP Horse Power ( 1 KW = 1.34 HP)','vehicle_category','Declined','Reason','Gross Vehicle Weight (Kgs)','Seating Capacity','body_type']
#master=master.loc[0:3371,:]
master_sub=master[["VEHICLE CODE",'Make Name','Model Name','Cubic Capacity','Fuel Type','Variant Name','Seating Capacity','Gross Vehicle Weight (Kgs)','BHP Horse Power ( 1 KW = 1.34 HP)','Declined', 'Reason']]
#master_sub["Make Model & Major Variant"]=master_sub["Make Model & Major Variant"].map(lambda x:x.lower(),)
#master_sub.index=master_sub["Make Model & Major Variant"]
master_sub['Fuel Type']=master_sub['Fuel Type'].apply(lambda x : x.strip())
master_sub['Cubic Capacity']=master_sub['Cubic Capacity'].astype('str')
#master_sub['Cubic Capacity']=master_sub['Cubic Capacity'].astype('str')
#master_sub['Fuel Type']=master_sub['Fuel Type'].map({'Diesel':'D','CNG':'CNG','Petrol':'P','Electric':'E','LPG':'LPG'})
master_sub["new"]=master_sub["Make Name"]+" "+master_sub["Model Name"]+" "+master_sub["Variant Name"]+" " +master_sub['Fuel Type'] +" "+ master_sub['Cubic Capacity']
master_sub['new']=master_sub['new'].astype("str")
#master_sub.index=master_sub["new"]
#df=pd.read_csv("pincode_master.csv",skiprows=[1])
#df["district_areaname"]=df["DISTRICT"]+df["AREA NAME"]
#df_sub=df[["district_areaname","PIN CODE"]]
#df_sub.index=df_sub['district_areaname']
#os.chdir(cwd)

def indexPosition(x):
    y=[]
    for item in x:
        make=item["make"].lower()
        model=item["model"].lower()
        variant=item["variant"].lower()
        searched=item["text"].lower()
        #print(make+" || "+model+" || "+variant+" --> "+searched)
        index=searched.find(" ")
        list=[]
        make_status=0
        model_status=0
        variant_status=0
        if(index==-1):
            value_status1=longestSubstringFinder(make,searched)
            value_status2=longestSubstringFinder(model,searched)
            value_status3=longestSubstringFinder(variant,searched)
            value_status=valueMax(value_status1,value_status2,value_status3)
            if(value_status==1 and make_status<value_status1):
                make_status=value_status1
            if(value_status==2 and model_status<value_status2):
                model_status=value_status2
            if(value_status==3 and variant_status<value_status3):
                variant_status=value_status3
        else:
            list=breakString(searched)
            for value in list:
                value_status1=longestSubstringFinder(make,value)
                value_status2=longestSubstringFinder(model,value)
                value_status3=longestSubstringFinder(variant,value)
                value_status=valueMax(value_status1,value_status2,value_status3)
                if(value_status==1 and make_status<value_status1):
                    make_status=value_status1
                if(value_status==2 and model_status<value_status2):
                    model_status=value_status2
                if(value_status==3 and variant_status<value_status3):
                    variant_status=value_status3
                if(value_status==0):
                    if(make_status<value_status1):
                        make_status=value_status1
                    elif(model_status<value_status2):
                        model_status=value_status2
                    elif(variant_status<value_status3):
                        variant_status=value_status3
        #print(str(make_status)+" "+str(model_status)+" "+str(variant_status))
        item["status"]=3*make_status*make_status+2*model_status*model_status+variant_status*variant_status
        item['status_string']=str(make_status)+","+str(model_status)+","+str(variant_status)

        #print(item["status"])
        y.append(item)
    return y
 


def valueMax(value_status1,value_status2,value_status3):
    if(value_status1>value_status2 and value_status1>value_status3):
        return 1
    if(value_status2>value_status1 and value_status2>value_status3):
        return 2
    if(value_status3>value_status2 and value_status3>value_status1):
        return 3
    return 0


def breakString(str):
    index=0;
    list=[]
    while(-1!=index):
        index=str.find(" ")
        #print(index)
        if(index!=-1):
            list.append(str[0:index])
        else:
            list.append(str)
        str=str[index+1:]
    return list



def longestSubstringFinder(S,T):
    m = len(S)
    n = len(T)
    counter = [[0]*(n+1) for x in range(m+1)]
    #print(counter)
    longest = 0
    lcs_set = set()
    for i in range(m):
        for j in range(n):
            if S[i] == T[j]:
                c = counter[i][j] + 1
                counter[i+1][j+1] = c
                if c > longest:
                    lcs_set = set()
                    longest = c
                    lcs_set.add(S[i-c+1:i+1])
                elif c == longest:
                    lcs_set.add(S[i-c+1:i+1])
    stringValue=""
    for item in lcs_set:
        stringValue=item
    return len(stringValue)

##implemented  for restricting the search
def breakStringlower(str): ## for restricting the saearch result 
    index=0;
    list=[]
    while(-1!=index):
        index=str.find(" ")
        #print(index)
        if(index!=-1):
            list.append((str[0:index]).lower())
        else:
            list.append((str).lower())
        str=str[index+1:]
    return list

def search_restrict(text_array,json_array):
    restriced_result=[]
    for json in json_array:
        z=json['concat'].lower()
        if all(x in z for x in text_array):
            restriced_result.append(json)
    return sorted(restriced_result , key=lambda elem: "%s %s %s " % (elem['make'],elem['model'], elem['variant']))

def reduce_string(s):
    repls = {'SEATER' : 'STR','AUTO TRANSMISSION':'AT','MANUAL TRANSMISSION':'MT','MANUAL':'MT','AUTOMATIC':'AT'}
    s=reduce(lambda a, kv: a.replace(*kv), repls.items(), s.upper())
    return s

@app.route('/vehicle/<string:text>',methods=['GET','HEAD'])
def scorer(text):
    #master_sub.index=master_sub["Make Model & Major Variant"]
    b=[]
    for found, score, matchrow in p.extract(reduce_string(text), master_sub["new"], limit=200,scorer=fuzzywuzzy.fuzz.partial_token_sort_ratio):# processor=lambda x:x):
        if score >40:
            a={}
            a["score"]=int(score)
            a["text"]=text
            #a["matched_string"]=found
            a["vehicleCode"]=int(master_sub.at[matchrow,'VEHICLE CODE'])
            a['variant']=str(master_sub.loc[matchrow,'Variant Name'])
            #a["ex_showroom_price"]=int(master_sub.at[matchrow,'Ex-Showroom Price in System'])
            a['make']=str(master_sub.loc[matchrow,'Make Name'])
            a['model']=str(master_sub.loc[matchrow,'Model Name'])
            #a['cubicCapacity']=int(master_sub.loc[matchrow,'Cubic Capacity'])
            a['fuelType']=str(master_sub.loc[matchrow,'Fuel Type'])
            #a['vehicleCategory']=
            #a['horsePowerBHP']=str(master_sub.loc[matchrow,'BHP Horse Power ( 1 KW = 1.34 HP)'])
            #a['grossVehicleWeight']=str(master_sub.loc[matchrow,'Gross Vehicle Weight (Kgs)'])
            a['isDeclined']=str(master_sub.loc[matchrow,'Declined'])
            a['declinedReason']=str(master_sub.loc[matchrow,'Reason'])
            
            #a['seatingCapacity']=str(master_sub.loc[matchrow,'Seating Capacity'])
            #a['bodyType']=
            #a["concat"]=str(master_sub.loc[matchrow,'Make Name']) + ' ' +str(master_sub.loc[matchrow,'Model Name']) + ' '+str(master_sub.loc[matchrow,'Variant Name']) + " "+str(master_sub.loc[matchrow,'Cubic Capacity'])
            a["concat"]=str(master_sub.loc[matchrow,'Make Name']) + ' ' +str(master_sub.loc[matchrow,'Model Name']) + ' '+str(master_sub.loc[matchrow,'Variant Name']) + " "+'('+str(master_sub.loc[matchrow,'Cubic Capacity'])+'cc'+')'+'('+str(master_sub.loc[matchrow,'Fuel Type'])+')'


            b.append(a)
        else:
            pass
    newlist = sorted(b, key=itemgetter('score'),reverse=True)
    w=search_restrict(breakStringlower(text),newlist)
    if len(w)==0:
        q=indexPosition(newlist)
        w=sorted(q,key=itemgetter('status'),reverse=True)
    #n_list=sorted(newlist,key=itemgetter('indexer'))
        #w=sorted(q , key=lambda elem: "%d %d" % (elem['score'], elem['status']),reverse=True)
    return jsonify(w)
@app.before_request
def before_request():
  g.start = time.time()

@app.after_request
def after_request(response):
    # This IF avoids the duplication of registry in the log,
    # since that 500 is already logged via @app.errorhandler.
    if response.status_code != 500:
        diff = time.time() - g.start
        ts = strftime('[%Y-%b-%d %H:%M]')
        logger = logging.getLogger('__name__')
        logger.error('%s %s %s %s %s %s %s',
                      ts,
                      diff,
                      request.remote_addr,
                      request.method,
                      request.scheme,
                      request.full_path,
                      response.status)
    return response

@app.errorhandler(Exception)
def exceptions(e):
    ts = strftime('[%Y-%b-%d %H:%M]')
    tb = traceback.format_exc()
    logger = logging.getLogger('__name__')
    logger.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s',
                  ts,
                  request.remote_addr,
                  request.method,
                  request.scheme,
                  request.full_path,
                  tb)
    return "Internal Server Error", 500
    #q=indexPosition(newlist)
    #w=sorted(q,key=itemgetter('status'),reverse=True)
    #n_list=sorted(newlist,key=itemgetter('indexer'))
    #sorted(b , key=lambda elem: "%d %s %s %s %s" % (elem['score'], elem['make'],elem['model'], elem['variant'],elem['fuel_type']))
    #return jsonify(w)


#routing for pincode generating function 
#@app.route('/pincode/<string:area>',methods=['GET'])

#def pincode_gen(area):
    #z=[]
    #text="".join(text.split())
    #for found, score, matchrow in p.extract(area.lower(), df_sub['district_areaname'],limit=6,scorer=fuzzywuzzy.fuzz.partial_ratio):# processor=lambda x:x):
        #if score >0:
            #a={}
            #a["score"]=str(score)
            #a["text"]=area
            #a["matched_string"]=found
            #a["pin_code"]=str(df_sub.at[found,'PIN CODE'])
            #z.append(a)
        #else:
            #z.append({"result":"no matches found"})

    #return jsonify(z)

    

if __name__ == '__main__':
    handler = RotatingFileHandler('app.log', maxBytes=5000000, backupCount=10)
    # getLogger('__name__') - decorators loggers to file / werkzeug loggers to stdout
    # getLogger('werkzeug') - werkzeug loggers to file / nothing to stdout
    logger = logging.getLogger('__name__')
    logger.setLevel(logging.ERROR)
    logger.addHandler(handler)
    app.run(host="10.20.16.203",port=81)



