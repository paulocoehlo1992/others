# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 17:45:51 2017 

@author: Vishal_Oben

"""
from pyflightdata import *
from pyflightdata import FlightData
api=FlightData() # importing the class file 
import pandas as pd
import numpy as np 
#import bs4
from pymongo import MongoClient
import time 
import random
import smtplib
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
client=MongoClient()
es=Elasticsearch("CONNECTION_STRING")

db=client.flight_data
collection_1=db.flight_data_collection


def elastic_processing(fetched):
    to_elastic=[]
    #to_not_elastic=[]
    for item in fetched:
        if (item['time']['real']['departure']==None) or(type(item['airport']['origin'])==str) or(item['time']['scheduled']['departure']==None) or(type(item['airport']['destination'])==str) or(item['identification']['number']['default']==None) or(item['airport']['destination']['position']['latitude']==None) or (item['airport']['origin']['position']['latitude']==None):
            pass#to_not_elastic.append(item)
        
        else:
            item["_id"]=item['identification']['number']['default']+'_'+str(item['time']['scheduled']['departure'])
            item["_index"]="sample_raw_flight_data"
            item["_type"]="flight_data_collection"
            item['airport']['destination']['position']['location']=str(item['airport']['destination']['position']['latitude'])+','+str(item['airport']['destination']['position']['longitude'])
            item['airport']['origin']['position']['location']=str(item['airport']['origin']['position']['latitude'])+','+str(item['airport']['origin']['position']['longitude'])
            if item['airport']['real']!=None:
                try:
                    item['airport']['real']['position']['location']=str(item['airport']['real']['position']['latitude'])+','+str(item['airport']['real']['position']['longitude'])
                except TypeError:
                    pass
            #m['_source']=json.dumps(item)
            to_elastic.append(item)
    return to_elastic
"""  k = ({
            "_index": "nginx",
            "_type" : "logs",
            "_id"   : idx,
            "_source": es_nginx_d,
         } for idx, es_nginx_d in decode_nginx_log(_nginx_fd))
 """    



def send_email(user, pwd, recipient, subject, body):
    import smtplib

    gmail_user = user
    gmail_pwd = pwd
    FROM = user
    
    TO = recipient if type(recipient) is list else [recipient]
    SUBJECT = subject
    TEXT = body

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print ('successfully sent the mail')
    except:
        print ("failed to send mail")
updated_tail=['VT-ITA', 'VT-ITB', 'VT-ITC', 'VT-ITD', 'VT-ITE', 'VT-ITF', 'VT-ITG', 'VT-ITH', 'VT-ITI', 'VT-ITJ', 'VT-ITK', 'VT-ITL', 'VT-ITM', 'VT-ITN', 'VT-ITO', 'VT-ITP', 'VT-ITQ', 'VT-ITR', 'VT-ITS', 'VT-ITT', 'VT-ITU', 'VT-ITV', 'VT-ITW', 'VT-ITX', 'VT-ITY', 'VT-ITZ', 'VT-IVA', 'VT-IVB', 'VT-IVC', 'VT-IVD', 'VT-IVE', 'VT-IVF', 'VT-IAL', 'VT-IAN', 'VT-IAO', 'VT-IAP', 'VT-IAQ', 'VT-IAR', 'VT-IAS', 'VT-IAX', 'VT-IAY', 'VT-IDC', 'VT-IDD', 'VT-IDE', 'VT-IDF', 'VT-IDG', 'VT-IDH', 'VT-IDI', 'VT-IDK', 'VT-IDL', 'VT-IDM', 'VT-IDN', 'VT-IDO', 'VT-IDP', 'VT-IDQ', 'VT-IDR', 'VT-IDS', 'VT-IDT', 'VT-IDU', 'VT-IDV', 'VT-IDW', 'VT-IDX', 'VT-IDY', 'VT-IDZ', 'VT-IDZ', 'VT-IEA', 'VT-IEB', 'VT-IEC', 'VT-IED', 'VT-IEE', 'VT-IEF', 'VT-IEG', 'VT-IEH', 'VT-IEI', 'VT-IEJ', 'VT-IEK', 'VT-IEL', 'VT-IEM', 'VT-IEN', 'VT-IEO', 'VT-IEP', 'VT-IEQ', 'VT-IER', 'VT-IES', 'VT-IET', 'VT-IEU', 'VT-IEV', 'VT-IEW', 'VT-IEX', 'VT-IEY', 'VT-IEZ', 'VT-IFA', 'VT-IFB', 'VT-IFC', 'VT-IFD', 'VT-IFE', 'VT-IFF', 'VT-IFG', 'VT-IFH', 'VT-IFI', 'VT-IFJ', 'VT-IFK', 'VT-IFL', 'VT-IFM', 'VT-IFN', 'VT-IFO', 'VT-IFP', 'VT-IFQ', 'VT-IFR', 'VT-IFS', 'VT-IFT', 'VT-IFU', 'VT-IFV', 'VT-IFW', 'VT-IFX', 'VT-IFY', 'VT-IFZ', 'VT-IGH', 'VT-IGI', 'VT-IGJ', 'VT-IGK', 'VT-IGL', 'VT-IGS', 'VT-IGT', 'VT-IGU', 'VT-IGV', 'VT-IGW', 'VT-IGX', 'VT-IGY', 'VT-IGZ', 'VT-IHA', 'VT-IHB', 'VT-IHC', 'VT-IHD', 'VT-IHE', 'VT-IHF', 'VT-IHG', 'VT-IHH', 'VT-IHJ', 'VT-IHK', 'VT-IHL', 'VT-IHN', 'VT-IHO', 'VT-IHP', 'VT-INP', 'VT-INQ', 'VT-INR', 'VT-INS', 'VT-INT', 'VT-INU', 'VT-INV', 'VT-INX', 'VT-INY', 'VT-INZ', 'VT-IYA', 'VT-IYB', 'VT-IYC', 'VT-IYD', 'VT-IYE', 'VT-IYF', 'VT-CID', 'VT-CIE', 'VT-CIF', 'VT-CIG', 'VT-CIH', 'VT-EXF', 'VT-EXG', 'VT-EXH', 'VT-EXI', 'VT-EXJ', 'VT-EXK', 'VT-EXL', 'VT-EXM', 'VT-EXT', 'VT-EXU', 'VT-EXV', 'VT-SCA', 'VT-SCB', 'VT-SCC', 'VT-SCF', 'VT-SCG', 'VT-SCH', 'VT-SCI', 'VT-SCJ', 'VT-SCK', 'VT-SCL', 'VT-SCM', 'VT-SCN', 'VT-SCO', 'VT-SCP', 'VT-SCQ', 'VT-SCR', 'VT-SCS', 'VT-SCT', 'VT-SCU', 'VT-SCV', 'VT-SCW', 'VT-SCX', 'VT-EDC', 'VT-EDD', 'VT-EDE', 'VT-EDF', 'VT-EPB', 'VT-EPC', 'VT-EPF', 'VT-EPG', 'VT-EPH', 'VT-EPI', 'VT-EPJ', 'VT-ESB', 'VT-ESC', 'VT-ESD', 'VT-ESE', 'VT-ESF', 'VT-ESG', 'VT-ESI', 'VT-ESJ', 'VT-ESL', 'VT-EXA', 'VT-EXB', 'VT-EXC', 'VT-EXD', 'VT-EXE', 'VT-PPA', 'VT-PPB', 'VT-PPD', 'VT-PPE', 'VT-PPF', 'VT-PPG', 'VT-PPH', 'VT-PPI', 'VT-PPJ', 'VT-PPK', 'VT-PPL', 'VT-PPM', 'VT-PPN', 'VT-PPO', 'VT-PPQ', 'VT-PPT', 'VT-PPU', 'VT-PPV', 'VT-PPW', 'VT-PPX', 'VT-EGJ', 'VT-ESO', 'VT-ESP', 'VT-EVA', 'VT-EVB', 'VT-ALF', 'VT-ALG', 'VT-ALH', 'VT-ALJ', 'VT-ALK', 'VT-ALL', 'VT-ALM', 'VT-ALN', 'VT-ALO', 'VT-ALP', 'VT-ALQ', 'VT-ALR', 'VT-ALS', 'VT-ALT', 'VT-ALU', 'VT-ALV', 'VT-ALW', 'VT-ALX', 'VT-ANA', 'VT-ANB', 'VT-ANC', 'VT-AND', 'VT-ANE', 'VT-ANG', 'VT-ANH', 'VT-ANI', 'VT-ANJ', 'VT-ANK', 'VT-ANL', 'VT-ANM', 'VT-ANN', 'VT-ANO', 'VT-ANP', 'VT-ANQ', 'VT-ANR', 'VT-ANS', 'VT-ANT', 'VT-ANU', 'VT-ANV', 'VT-ANW', 'VT-ANX', 'VT-ANY', 'VT-ANZ', 'VT-NAA', 'VT-NAC', 'VT-AXH', 'VT-AXI', 'VT-AXJ', 'VT-AXM', 'VT-AXN', 'VT-AXP', 'VT-AXQ', 'VT-AXR', 'VT-AXT', 'VT-AXU', 'VT-AXW', 'VT-AXX', 'VT-AXZ', 'VT-AYA', 'VT-AYB', 'VT-AYC', 'VT-AYD', 'VT-GHA', 'VT-GHB', 'VT-GHC', 'VT-GHD', 'VT-GHE', 'VT-GHF', 'VT-SLA', 'VT-SLB', 'VT-SGG', 'VT-SGH', 'VT-SGJ', 'VT-SGQ', 'VT-SGV', 'VT-SGX', 'VT-SGY', 'VT-SGZ', 'VT-SLE', 'VT-SLF', 'VT-SLG', 'VT-SLH', 'VT-SLI', 'VT-SLJ', 'VT-SLL', 'VT-SLM', 'VT-SLN', 'VT-SLO', 'VT-SPF', 'VT-SPK', 'VT-SPP', 'VT-SZA', 'VT-SZB', 'VT-SZI', 'VT-SZJ', 'VT-SZK', 'VT-SZM', 'VT-SZN', 'VT-SLC', 'VT-SLD', 'VT-SPU', 'VT-SZL', 'VT-SUA', 'VT-SUB', 'VT-SUC', 'VT-SUD', 'VT-SUE', 'VT-SUF', 'VT-SUG', 'VT-SUH', 'VT-SUI', 'VT-SUJ', 'VT-SUK', 'VT-SUL', 'VT-SUM', 'VT-SUO', 'VT-SUP', 'VT-SUQ', 'VT-SUR', 'VT-SUS', 'VT-SUT', 'VT-SUU', 'VT-SUV', 'VT-SUW', 'VT-SUX', 'VT-SUY', 'VT-JWP', 'VT-JWQ', 'VT-JWV', 'VT-JWW', 'VT-JWR', 'VT-JWS', 'VT-JWT', 'VT-JWU', 'VT-JCJ', 'VT-JCK', 'VT-JCL', 'VT-JCM', 'VT-JCN', 'VT-JCP', 'VT-JCQ', 'VT-JCR', 'VT-JCS', 'VT-JCT', 'VT-JCU', 'VT-JCV', 'VT-JCW', 'VT-JGX', 'VT-JGY', 'VT-JBB', 'VT-JBC', 'VT-JBD', 'VT-JBE', 'VT-JBF', 'VT-JBG', 'VT-JBH', 'VT-JBJ', 'VT-JBK', 'VT-JBL', 'VT-JBM', 'VT-JBN', 'VT-JBP', 'VT-JBQ', 'VT-JBR', 'VT-JBS', 'VT-JBU', 'VT-JBV', 'VT-JBW', 'VT-JBX', 'VT-JFA', 'VT-JFB', 'VT-JFC', 'VT-JFD', 'VT-JFE', 'VT-JFF', 'VT-JFG', 'VT-JFH', 'VT-JFJ', 'VT-JFK', 'VT-JFL', 'VT-JFM', 'VT-JFN', 'VT-JFP', 'VT-JFQ', 'VT-JFR', 'VT-JFS', 'VT-JFT', 'VT-JFW', 'VT-JFX', 'VT-JFY', 'VT-JFZ', 'VT-JGA', 'VT-JGE', 'VT-JGF', 'VT-JGG', 'VT-JGJ', 'VT-JGK', 'VT-JGP', 'VT-JGQ', 'VT-JGR', 'VT-JGS', 'VT-JGT', 'VT-JGU', 'VT-JGV', 'VT-JGW', 'VT-JTA', 'VT-JTB', 'VT-JTC', 'VT-JTD', 'VT-JTE', 'VT-JTF', 'VT-JTG', 'VT-JTH', 'VT-JTK', 'VT-JTL', 'VT-JTM', 'VT-JTN', 'VT-JBY', 'VT-JBZ', 'VT-JGC', 'VT-JGD', 'A6-JAE', 'VT-JEH', 'VT-JEK', 'VT-JEM', 'VT-JEQ', 'VT-JES', 'VT-JET', 'VT-JEU', 'VT-JEV', 'VT-JEW', 'VT-JEX', 'VT-WGA', 'VT-WGB', 'VT-WGC', 'VT-WGD', 'VT-WGE', 'VT-WGF', 'VT-WGG', 'VT-WGH', 'VT-WGI', 'VT-WGJ', 'VT-WGK', 'VT-WGL', 'VT-WGM', 'VT-GOI', 'VT-GOJ', 'VT-GOK', 'VT-GOL', 'VT-GOM', 'VT-GON', 'VT-GOO', 'VT-GOP', 'VT-GOQ', 'VT-GOR', 'VT-GOS', 'VT-GOT', 'VT-WAF', 'VT-WAG', 'VT-WAH', 'VT-WAI', 'VT-WAJ', 'VT-WAK', 'VT-WAL', 'VT-JDC', 'VT-JDD', 'VT-JCX', 'VT-JCY', 'VT-JCZ', 'VT-SIZ', 'VT-SJA', 'VT-JLE', 'VT-JLF', 'VT-SJI', 'VT-SJJ', 'VT-JLH', 'VT-JLJ', 'VT-TNB', 'VT-TNC', 'VT-TNE', 'VT-TNF', 'VT-TNH', 'VT-TNI', 'VT-TNJ', 'VT-TTB', 'VT-TTC', 'VT-TTD', 'VT-TTE', 'VT-TTF', 'VT-TTG', 'VT-TTH', 'VT-TTI', 'VT-TTJ', 'VT-TTK', 'VT-TTL', 'VT-TTM', 'VT-TTN', 'VT-TMK', 'VT-TMP', 'VT-TMU', 'VT-TMC', 'VT-TMR', 'VT-ABD', 'VT-ABA', 'VT-ABB', 'VT-AII', 'VT-AIT', 'VT-AIU', 'VT-AIV', 'VT-AIW', 'VT-AIX', 'VT-AIY', 'VT-AIZ', 'VT-RKC', 'VT-RKD', 'VT-RKE', 'VT-RKF', 'VT-RKG', 'VT-RKH', 'VT-HYD', 'VT-PNQ', 'VT-IMP', 'VT-JRT', 'VT-IXR', 'VT-BLR', 'VT-APJ', 'VT-SXR', 'VT-CCU', 'VT-MOD', 'VT-DEL', 'VT-IXC', 'VT-RED', 'VT-ATF', 'VT-ATB']
#updated_tail=['VT-WGI','VT-TMK','VT-TMP','VT-TMU','VT-TMC','VT-TMR','VT-ABD','VT-ABA','VT-ABB','VT-AII','VT-AIT','VT-AIU','VT-AIV','VT-AIW','VT-AIX','VT-AIY','VT-AIZ','VT-RKC','VT-RKD','VT-RKE','VT-RKF','VT-RKG','VT-RKH','VT-TNB','VT-TTN','VT-TMK','VT-TMP','VT-TMU','VT-TMC','VT-TMR','VT-GHB', 'VT-IGK', 'VT-AII', 'VT-JFW', 'VT-IAX', 'VT-ITY', 'VT-JCN', 'VT-ITJ', 'VT-SPK', 'VT-PPM', 'VT-DEL', 'VT-SCP', 'VT-SLM', 'VT-ITU', 'VT-SLJ', 'VT-SCC', 'VT-ITL', 'VT-JEH', 'VT-SLB', 'VT-JBP', 'VT-PPL', 'VT-IFO', 'VT-SLF', 'VT-EDE', 'VT-AXJ', 'VT-WAI', 'VT-IDY', 'VT-IEH', 'VT-ITD', 'VT-JCL', 'VT-IHA', 'VT-SPF', 'VT-JTA', 'VT-WAG', 'VT-SGZ', 'VT-INP', 'VT-IGJ', 'VT-PPU', 'VT-ANR', 'VT-JEV', 'VT-EXE', 'VT-JGU', 'VT-IDO', 'VT-SZK', 'VT-SLA', 'VT-EXA', 'VT-ALQ', 'VT-IHC', 'VT-IES', 'VT-JBQ', 'VT-ITW', 'VT-JBF', 'VT-JGV', 'VT-TTI', 'VT-ANE', 'VT-JTE', 'VT-IDR', 'VT-SZM', 'VT-ANU', 'VT-IFS', 'VT-ITM', 'VT-JWP', 'VT-GHA', 'VT-ABA', 'VT-IGH', 'VT-ESP', 'VT-TTK', 'VT-IAP', 'VT-SZL', 'VT-INT', 'VT-INY', 'VT-JWR', 'VT-JFP', 'VT-ANZ', 'VT-SUT', 'VT-JEQ', 'VT-JGP', 'VT-IHF', 'VT-SLI', 'VT-ALO', 'VT-SUM', 'VT-MOD', 'VT-INX', 'VT-JGK', 'VT-PPF', 'VT-INZ', 'VT-IDA', 'VT-ALK', 'VT-IFL', 'VT-SCI', 'VT-IAL', 'VT-IFP', 'VT-EXF', 'VT-SUG', 'VT-IDH', 'VT-SZN', 'VT-JCJ', 'VT-IVC', 'VT-SUU', 'VT-GOO', 'VT-IEK', 'VT-JEX', 'VT-IFI', 'VT-SCR', 'VT-JFA', 'VT-JNL', 'VT-SPU', 'VT-PPH', 'VT-EDD', 'VT-PPK', 'VT-JBR', 'VT-WGA', 'VT-IFQ', 'VT-WGD', 'VT-JBL', 'VT-AIX', 'VT-JBM', 'VT-SGG', 'VT-SUP', 'VT-IEQ', 'VT-GHD', 'VT-ITI', 'VT-SZB', 'VT-IDL', 'VT-SLC', 'VT-IEY', 'VT-SUA', 'VT-ITA', 'VT-SCG', 'VT-IDC', 'VT-IFR', 'VT-JCM', 'VT-JFD', 'VT-BLR', 'VT-EPB', 'VT-JCP', 'VT-IFA', 'VT-ITH', 'VT-SUR', 'VT-WAJ', 'VT-ITN', 'VT-AXX', 'VT-SUL', 'VT-JCY', 'VT-JFZ', 'VT-TTG', 'VT-IEA', 'VT-EPI', 'VT-SUY', 'VT-IEE', 'VT-ALN', 'VT-ALR', 'VT-IDB', 'VT-ALT', 'VT-AIZ', 'VT-SUS', 'VT-IEP', 'VT-JRT', 'VT-JGW', 'VT-IFG', 'VT-ITE', 'VT-TTD', 'VT-JWV', 'VT-SUV', 'VT-IAY', 'VT-ANS', 'VT-JGJ', 'VT-IGY', 'VT-ITQ', 'VT-IDG', 'VT-GOT', 'VT-ALS', 'VT-IFF', 'VT-IGU', 'VT-ESJ', 'VT-SUK', 'VT-JLJ', 'VT-WGH', 'VT-ESB', 'VT-TTE', 'VT-JGD', 'VT-SLG', 'VT-JGQ', 'VT-AXM', 'VT-IEX', 'VT-RED', 'VT-RKG', 'VT-GOS', 'VT-ANX', 'VT-AYB', 'VT-ITF', 'VT-PPB', 'VT-ANH', 'VT-ANL', 'VT-ANN', 'VT-JGG', 'VT-AXZ', 'VT-ITR', 'VT-SUE', 'VT-TNE', 'VT-JDC', 'VT-PPW', 'VT-INU', 'VT-GOM', 'VT-ESE', 'VT-JCT', 'VT-SGH', 'VT-EPH', 'VT-ANW', 'VT-ITZ', 'VT-ABB', 'VT-ITC', 'VT-WAK', 'VT-SUH', 'VT-SCN', 'VT-RKF', 'VT-EPG', 'VT-APJ', 'VT-SXR', 'VT-IDE', 'VT-JFY', 'VT-AXI', 'VT-SCO', 'VT-JTH', 'VT-GHF', 'VT-EGJ', 'VT-EDC', 'VT-JCV', 'VT-SCW', 'VT-IDD', 'VT-SUJ', 'VT-ITV', 'VT-CIG', 'VT-INQ', 'VT-JCQ', 'VT-JBJ', 'VT-SUD', 'VT-JBY', 'VT-IGW', 'VT-EPJ', 'VT-JBV', 'VT-SCH', 'VT-TTL', 'VT-IDN', 'VT-ITK', 'VT-SJA', 'VT-JLB', 'VT-IHG', 'VT-IDT', 'VT-IDK', 'VT-JBD', 'VT-ANJ', 'VT-IGL', 'VT-SPP', 'VT-IDX', 'VT-IET', 'VT-JFM', 'VT-GOJ', 'VT-JGC', 'VT-JBG', 'VT-SCL', 'VT-IGT', 'VT-ALG', 'VT-ANI', 'VT-GHE', 'VT-IDU', 'VT-IFU', 'VT-SCK', 'VT-SLH', 'VT-SCU', 'VT-IEW', 'VT-JCZ', 'VT-AXP', 'VT-JWS', 'VT-ALU', 'VT-SGX', 'VT-IEC', 'VT-AIV', 'VT-EVB', 'VT-JFS', 'VT-AYC', 'VT-IHE', 'VT-JTD', 'VT-TTC', 'VT-IFH', 'VT-SCQ', 'VT-IFW', 'VT-ITT', 'VT-AXR', 'VT-JBK', 'VT-EXB', 'VT-GHC', 'VT-SGJ', 'VT-ESF', 'VT-TTH', 'VT-IFM', 'VT-JGY', 'VT-SCB', 'VT-CCU', 'VT-IEJ', 'VT-JDD', 'VT-SJI', 'VT-SGQ', 'VT-PPA', 'VT-AIW', 'VT-TNC', 'VT-IEG', 'VT-TTN', 'VT-CIF', 'VT-JBC', 'VT-PPX', 'VT-ANP', 'VT-EXI', 'VT-JGA', 'VT-JBN', 'VT-JEM', 'VT-PPO', 'VT-CMA', 'VT-IFY', 'VT-SCA', 'VT-SZJ', 'VT-AXW', 'VT-SUO', 'VT-WAF', 'VT-CID', 'VT-JBX', 'VT-AXQ', 'VT-PPG', 'VT-SUQ', 'VT-SCV', 'VT-IFN', 'VT-JFX', 'VT-AND', 'VT-AIU', 'VT-IEV', 'VT-IDF', 'VT-ANA', 'VT-IDI', 'VT-IYA', 'VT-SGV', 'VT-SLD', 'VT-EXV', 'VT-IHB', 'VT-ESG', 'VT-JES', 'VT-IFT', 'VT-PPT', 'VT-JBE', 'VT-TTM', 'VT-SLE', 'VT-IDQ', 'VT-JBU', 'VT-ANB', 'VT-ANT', 'VT-SUW', 'VT-IDS', 'VT-ITO', 'VT-JGT', 'VT-JLH', 'VT-PPQ', 'VT-PNQ', 'VT-IAQ', 'VT-PPV', 'VT-ANC', 'VT-RKE', 'VT-JFG', 'VT-ITB', 'VT-IFV', 'VT-SCX', 'VT-JEU', 'VT-WGC', 'VT-ESC', 'VT-EXC', 'VT-ESI', 'VT-JFF', 'VT-IGV', 'VT-ANK', 'VT-WGE', 'VT-JTF', 'VT-JLE', 'VT-ESL', 'VT-IHD', 'VT-JTB', 'VT-JCW', 'VT-WGJ', 'VT-SUF', 'VT-PPE', 'VT-HYD', 'VT-EXG', 'VT-GON', 'VT-ATB', 'VT-IAN', 'VT-SZA', 'VT-JCS', 'VT-RKD', 'VT-JLF', 'VT-GOI', 'VT-JFK', 'VT-SCJ', 'VT-WGF', 'VT-JTC', 'VT-ITP', 'VT-WGM', 'VT-JGE', 'VT-PPI', 'VT-NAC', 'VT-JFN', 'VT-JEK', 'VT-AXH', 'VT-ESO', 'VT-JCX', 'VT-RKC', 'VT-JBS', 'VT-JGR', 'VT-JFQ', 'VT-GOQ', 'VT-ABD', 'VT-ATF', 'VT-IGI', 'VT-ANY', 'VT-JBB', 'VT-EXD', 'VT-GOL', 'VT-IFX', 'VT-JGS', 'VT-IDW', 'VT-IEO', 'VT-SGY', 'VT-AXT', 'VT-JFB', 'VT-SCF', 'VT-JFC', 'VT-SIZ', 'VT-INS', 'VT-AYD', 'VT-AIY', 'VT-IFB', 'VT-IFD', 'VT-IFJ', 'VT-IDP', 'VT-IEM', 'VT-GOK', 'VT-JBZ', 'VT-ITS', 'VT-ALJ', 'VT-IDZ', 'VT-IEI', 'VT-JET', 'VT-JCU', 'VT-INV', 'VT-IEN', 'VT-SCS', 'VT-SJJ', 'VT-ALM', 'VT-TTB', 'VT-TNB', 'VT-ITG', 'VT-JFH', 'VT-ALL', 'VT-IFK', 'VT-EXT', 'VT-GOP', 'VT-ITX', 'VT-AXN', 'VT-WGB', 'VT-EVA', 'VT-JFR', 'VT-SUI', 'VT-SLL', 'VT-JFJ', 'VT-IFE', 'VT-EDF', 'VT-PPN', 'VT-AIT', 'VT-JCR', 'VT-IFZ', 'VT-TTJ', 'VT-ALH', 'VT-CIE', 'VT-ESD', 'VT-AYA', 'VT-SUC', 'VT-JWW', 'VT-IEZ', 'VT-JWT', 'VT-SLN', 'VT-IED', 'VT-SZI', 'VT-SUB', 'VT-EPF', 'VT-IDM', 'VT-ALP', 'VT-SCM', 'VT-IFC', 'VT-JWU', 'VT-ANQ', 'VT-IGS', 'VT-JEW', 'VT-IAO', 'VT-WAL', 'VT-IAR', 'VT-JFE', 'VT-PPD', 'VT-JCK', 'VT-IGX', 'VT-JGF', 'VT-IGZ', 'VT-JBH', 'VT-IEL', 'VT-TTF', 'VT-JWQ', 'VT-WAH', 'VT-PPJ', 'VT-EXU', 'VT-ANM', 'VT-NAA', 'VT-IXC', 'VT-JGX', 'VT-ALF', 'VT-IAS', 'VT-AXU', 'VT-WGG', 'VT-SCT', 'VT-IDV', 'VT-IEF', 'VT-GOR', 'VT-JFL', 'VT-ANO', 'VT-ANG', 'VT-IER', 'VT-JFT', 'VT-EPC', 'A6-JAE', 'VT-JBW', 'VT-ANV', 'VT-SLO', 'VT-IDJ', 'VT-IEB', 'VT-INR', 'VT-IEU']

while True:
    time.sleep(86400*3)

    #es=Elasticsearch('https://vpc-digit-test-es-zhvoov6ksa5umlzulscdwd55ua.ap-south-1.es.amazonaws.com')
    client=MongoClient()
    print("waiting for next iteration")
    #print("program is waiting")
    #time.sleep(86400*1)
    fetched=[]
    to_elastic_db=[]
    fetched_tail=[]
    unfetched=[]
    #print("program is waiting")
    #time.sleep(259200)
    for tail in set(updated_tail):
        data=api.get_history_by_tail_number(tail)
        time.sleep(3)
        print(len(data),tail)
        #helpers.bulk(es,elastic_processing(data), chunk_size=5000)
        time.sleep(random.randint(1,5))
    
        if len(data)>0:
            #to_elastic_db.extend(data)
            df=pd.DataFrame(data)
            fetched_tail.append(tail)
            fetched+=df.to_dict('records')
            time.sleep(3)
            print(len(fetched))
        else:
            unfetched.append(tail)
            time.sleep(1)
            print(len(unfetched))
        #print(len(list(collection_1.find())))
    
#collection_1.insert_many(df.to_dict('records'))  
    collection_1.insert_many(fetched)
    print("inserted to mongoDB")
    time.sleep(4)
    #print(elastic_processing(fetched))
    #helpers.bulk(es,elastic_processing(to_elastic_db), chunk_size=5000)
    #print("inserted to ES")
#print(len(list(collection_1.find({})))) 
    print(unfetched)
    x=[]

    for item in unfetched:
        data=api.get_history_by_tail_number(item)
        #helpers.bulk(es,elastic_processing(data), chunk_size=5000)
        time.sleep(5)
        print(len(data),item)
        if len(data)>0:
            df=pd.DataFrame(data)
            x+=df.to_dict('records')
    if len(x)>0:
        collection_1.insert_many(x)
        #helpers.bulk(es,elastic_processing(x), chunk_size=5000)

    current_time=str(time.ctime())
    name_of_fetched=" ,".join(fetched_tail)
    messg="the latest iteration completed at"+":"+current_time+"Total number of fetched tail-ids:"+str(len(fetched_tail))+"Total number of unfetched tail ids :"+str(len(unfetched))+"----"+"Tail_ids:"+" "+name_of_fetched
    send_email("vishalvikram1992","password",["vishal.nestaway@gmail.com","vishalvikram.singh@godigit.com"],"flight_data",messg)  
    import sqlalchemy
    import datetime
    from sqlalchemy import *
    engine = sqlalchemy.create_engine("postgresql://USERNAME:PASSWORD@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/DB_NAME")
    engine.connect()
    s="INSERT INTO dm_reports.m_audit_log (job_id,message_class,trace_message,job_name,origin_ref,time_stamp)VALUES('00002','successful','flight_data_fetched','flightradar24','python',to_char(now(),'DD-MM-YYYY HH24:MI:SS'));"#.format(k)
    engine.execute(s)
    time.sleep(86400*3)
