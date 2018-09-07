'''
-------------------------------------------------------------------------
Created on 18-Nov-2017
@author: Digit-Robotics Team
Created for accessing IIB portal using Automation
-------------------------------------------------------------------------
'''

#importing  modules
import base64
import os
import requests

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pickle
import openpyxl # manipulating XL files
import random

import configparser
import subprocess
import logging
import time
import re
from selenium.webdriver import DesiredCapabilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
import pandas as pd # data wrangling open source tool
import sqlalchemy # interacting with DB
# create a file handler

timestr = time.strftime("%Y%m%d-%H%M%S")
handler = logging.FileHandler('IIB_'+str(timestr)+'.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)
   
'''
---------------------------------------------------------------------------
Reading Properties file
---------------------------------------------------------------------------
'''

config = configparser.RawConfigParser()
config.read('ConfigFile.properties')




'''
---------------------------------------------------------------------------
AgenPortal URL-Can be changed based on the environment
Assigning value at the time of load, it will not be called everytime.

---------------------------------------------------------------------------
'''
url=config.get('LoginCredentials', 'portal.url')
#userid=config.get('LoginCredentials', 'portal.username') #Encrypt UserId 

#pwd=config.get('LoginCredentials', 'portal.password') # Encrypt password 
userid='27363' # userid
#pwd=config.get('LoginCredentials', 'portal.password') # Encrypt password 
pwd='Admin@123'
'''
---------------------------------------------------------------------------
Reading Global Variables
---------------------------------------------------------------------------
'''
cookies_dict = {}
iib_header=['S.No:','Insurer Name','Registration Number','Chassis Number','Engine Number',
            'Make Code','Model Code','Accident Loss Date','Claim Intimation Date',
            'Accident Place','Total OD Claims Paid Amount','OD Open Provision',
            'OD Close Provision','Total TP Claims Paid Amount','TP Open Provision',
            'TP Close Provision']


'''
---------------------------------------------------------------------------
Base64 file/url transformation before sending to Google API for OCR
---------------------------------------------------------------------------
'''
def get_as_base64(url):
    return base64.b64encode(requests.get(url).content)

def detect_text(image_file, access_token=None):

    with open(image_file, 'rb') as image:
        base64_image = base64.b64encode(image.read()).decode()

    url = 'https://vision.googleapis.com/v1/images:annotate?key={}'.format(access_token)
    header = {'Content-Type': 'application/json'}
    body = {
        'requests': [{
            'image': {
                'content': base64_image,
            },
            'features': [{
                'type': 'TEXT_DETECTION',
                'maxResults': 1,
            }]

        }]
    }
    response = requests.post(url, headers=header, json=body).json()
    text = response['responses'][0]['textAnnotations'][0]['description'] if len(response['responses'][0]) > 0 else ''
    
    return text

'''
---------------------------------------------------------------------------
Save and load cookies which will be pushed to next request to keep both page in sync
---------------------------------------------------------------------------
'''
def save_cookies(session, filename):
    if not os.path.isdir(os.path.dirname(filename)):
        return False
    with open(filename, 'w') as f:
        f.truncate()
        pickle.dump(session.cookies._cookies, f)


def load_cookies(session, filename):
    if not os.path.isfile(filename):
        return False

    with open(filename) as f:
        cookies = pickle.load(f)
        if cookies:
            jar = requests.cookies.RequestsCookieJar()
            jar._cookies = cookies
            session.cookies = jar
        else:
            return False
    
'''
---------------------------------------------------------------------------
Gateway to trigger Gooogle Vision API 
---------------------------------------------------------------------------
'''    
def detect_text_fromURL(image_file, access_token=None):


    respon=requests.get(image_file,cookies=cookies_dict)    

    base64_image = base64.b64encode(respon.content).decode()
    base64_imagecopy=base64_image

    with open("imageToSave.png", "wb") as fh:
        fh.write(base64.decodebytes(base64_imagecopy.encode('utf-8')))
    
        
    url = 'https://vision.googleapis.com/v1/images:annotate?key={}'.format('AIzaSyCTi9Nt5NO7HGTh3W1STyyymKjcpzzNIkM')
    header = {'Content-Type': 'application/json'}
    body = {
        'requests': [{
            'image': {
                'content': base64_image,
            },
            'features': [{
                'type': 'TEXT_DETECTION',
                'maxResults': 1,
            }]

        }]
    }
    response = requests.post(url, headers=header, json=body).json()
    text = response['responses'][0]['textAnnotations'][0]['description'] if len(response['responses'][0]) > 0 else ''
    print(text)
    #return text
#print(detect_text("royal.jpg", 'AIzaSyCf7P6fOaQV65AKvbc8a9VWUhBCV9U9PGQ'))
def isBlank (myString):
    return not (myString and myString.strip())


# required function 

def callIIBURL():
    user_agent_list=["user-agent=Pen Pineapple apple pen","user-agent=Pen Pineapple mango pen","user-agent=Pen orange apple pen"]
    
    options = Options()
    options.add_argument('--disable-infobars')
    options.add_argument(random.choice(user_agent_list))

    

    driver = webdriver.Chrome(executable_path=r'C:\Users\vishal\Downloads\chromedriver_win32\chromedriver.exe',chrome_options=options) # path to driver executable
    
    driver.get(url)
    #pickle.dump( driver.get_cookies() , open("cookies.txt","wb"))
    cookies_list = driver.get_cookies()
    
    for cookie in cookies_list:
        cookies_dict[cookie['name']] = cookie['value']
    
    driver.maximize_window()
    time.sleep(3) # wait 
    
    useridxpath=driver.find_element_by_xpath('//*[@id="swUsername"]')
    useridxpath.clear()  
    useridxpath.send_keys(userid)
    time.sleep(3)
    passwordxpath=driver.find_element_by_xpath('//*[@id="swPassword"]')
    passwordxpath.clear()  
    passwordxpath.send_keys(pwd)
    time.sleep(3)
    captcha_xpath='//*[@id="jcaptcha_txt"]'
    useridControl = driver.find_element_by_xpath(captcha_xpath)
    useridControl.clear()    
    #captchaval=detect_text_fromURL("https://iib.gov.in/IIB/jcaptcha.jpg",'AIzaSyCf7P6fOaQV65AKvbc8a9VWUhBCV9U9PGQ')
    text = input("Enter captcha")

    
    useridControl.send_keys(text)
    
    time.sleep(5)
    
    buttoncontrol = driver.find_element_by_xpath("/html/body/div/table/tbody/tr/td/form/table/tbody/tr[7]/td/table/tbody/tr/td/a[1]/img")
      
    buttoncontrol.click()
    
    time.sleep(5)
    
    acceptControl=driver.find_element_by_xpath('/html/body/form/div/table[3]/tbody/tr[3]/td/input[1]')
    acceptControl.click()
    
    time.sleep(5)
    
    #registration_number=['UP64L4977','UP64L4978']
    #registration_number_filename = "reg_no.xlsx"
    #book = openpyxl.load_workbook(filename=registration_number_filename)
    #sheet = book.get_sheet_by_name('IIB')
    #sheet_data=book.get_sheet_by_name('Reg')
    #sheet = book.active
    #counter=1


    while True:
        #print("waiting for next iteration")
        #time.sleep(24*60*60)
        engine = sqlalchemy.create_engine("postgresql://dbmaster:D2b!p0snt@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
        engine.connect()
        time.sleep(4)
        X="select distinct policy_number, veh_reg_no,max(curr_ncb) curr_ncb, max( prev_ncb) prev_ncb ,max(prev_policy_expiry) prev_policy_expiry from dm_marts.m_policy where  product_name='Digit Private Car Policy' and curr_ncb>0 and policy_issue_date::date > current_date::date-32 group by policy_number, veh_reg_no"
        #x="select distinct policy_number, veh_reg_no,max(curr_ncb) curr_ncb, max( prev_ncb) prev_ncb ,max(prev_policy_expiry) prev_policy_expiry from dm_marts.m_policy where  product_name='Digit Private Car Policy' and curr_ncb>0 and policy_issue_date::date=current_date::date-1 group by policy_number, veh_reg_no"
        #S="select distinct policy_number, veh_reg_no, max(curr_ncb), max(prev_ncb) , max(prev_policy_expiry) 
        #from dm_marts.m_policy where  product_name='Digit Private Car Policy' and curr_ncb>0  and policy_issue_date::date=current_date::date-1"
        #s="select distinct policy_number, veh_reg_no,curr_ncb, prev_ncb , prev_policy_expiry from dm_marts.m_policy where  product_name='Digit Private Car Policy' and curr_ncb>0"
        df=pd.read_sql(X,con=engine)
        print(df.shape)
        print("data is read from the postgres")
        time.sleep(3)
    

    
    #firstrow=True
    #cells = sheet[sheet.dimensions]
        for data in list(df.itertuples())[0:]:
            print(data)
            print("iterating")
            #https://nonlife.iib.gov.in/IIB/Login_2.jsp
            driver.get("https://nonlife.iib.gov.in/IIB/claimHistoryReportsAction.do?method=GetClaimHistoryReport&RegNo="+data[2])
            time.sleep(1)
            number_of_records_control=driver.find_element_by_xpath('//*[@id="div1"]/table/tbody/tr/td/b/font')
            number_of_records=re.findall(r'\d+',str(number_of_records_control.text))
            logger.info('%s - %s -%s','Number of records==  ',number_of_records,data[2])
            print("Number of records==",number_of_records)
            if(int(number_of_records[0])>0):

                    #engine = sqlalchemy.create_engine("postgresql://dbmaster:D2b!p0snt@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
                time.sleep(1)    
                calls_d=pd.read_html(driver.page_source,attrs={'id': 'table0'})
                time.sleep(1)
            #print(type(calls_df))
                calls_df=calls_d[0]
                print(type(calls_df))
                print(calls_df.columns)
                calls_df.columns = calls_df.iloc[0] #
                print(calls_df.columns)
                calls_df.drop(0,axis=0,inplace=True)
                time.sleep(1)
            #calls_df.reindex(calls_df.index.drop(0))
        

                calls_df["policy_number"]=data.policy_number
                calls_df['veh_reg_no']=data.veh_reg_no
                calls_df['curr_ncb']=data[3]
                calls_df['prev_ncb']=data[4]
                calls_df["prev_policy_expiry"]=data.prev_policy_expiry
                print(calls_df)
                time.sleep(5)
                calls_df['Make Code']=calls_df['Make Code'].astype('str')
                calls_df['Model Code']=calls_df['Model Code'].astype('str')
                calls_df=calls_df.astype('str')




                #calls_df.to_sql(name="iib_records_01",if_exists="append",con=engine,schema="dm_marts")

                    #list_to_json=calls_df[0].to_json(orient='records')
                    #time.sleep(1)
                    #print(list_to_json)
                    #logger.info(list_to_json)
                    #sheet['B'+str(counter)]=str(list_to_json)
                    #time.sleep(1)
                    #book.save(registration_number_filename)
                    #time.sleep(5)                      
            else:
                dict_null={'Accident Loss Date':'NA','Accident Place':'NA','Chassis Number':'NA','Claim Intimation Date':'NA','Engine Number':'NA','Insurer Name':'NA','Make Code':'NA','Model Code':'NA','OD Close Provision':'NA','OD Open Provision':'NA','Registration Number':'NA','S.No:':'NA','TP Close Provision':'NA','TP Open Provision':'NA','Total OD Claims Paid Amount':'NA','Total TP Claims Paid Amount': 'NA'}
                calls_no_df=pd.DataFrame(dict_null,index=[0])
                calls_no_df["policy_number"]=data.policy_number
                calls_no_df['veh_reg_no']=data.veh_reg_no
                calls_no_df['curr_ncb']=data[3]
                calls_no_df['prev_ncb']=data[4]
                calls_no_df['prev_policy_expiry']=data.prev_policy_expiry
                calls_no_df=calls_no_df.astype('str')

                #calls_no_df.to_sql(name="iib_records_01",if_exists="append",con=engine,schema="dm_marts")
             #engine = sqlalchemy.create_engine("postgresql://dbmaster:D2b!p0snt@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
            
                    #sheet['B'+str(counter)]='No Claim record found for this vehicle during the last 5 years'
                    #book.save(registration_number_filename)
                    #time.sleep(1)    
                
                time.sleep(1)
        #for i in range(23*30):
            #driver.get("https://iib.gov.in/IIB/claimHistoryReportsAction.do?method=GetClaimHistoryReport&RegNo="+'HR26CB0219')
            #number_of_records_control=driver.find_element_by_xpath('//*[@id="div1"]/table/tbody/tr/td/b/font')
            #number_of_records=re.findall(r'\d+',str(number_of_records_control.text))
            #logger.info('%s - %s -%s','Number of records==  ',number_of_records,data[2])
            #print("Number of records==",number_of_records)
            #print("waiting is activated"+" "+ str(i))
            #time.sleep(2*60)
        #except Exception as ee:
                    #print(ee)
        #print("waiting for next iteration")            #logger.info('%s - %s -%s','Exception during finding the table data or assigning json to column for  ',data[0].value,ee)
        #time.sleep(24*60*60)            #book.save(registration_number_filename)
            

    
if __name__ == '__main__':
    callIIBURL()
#https://iib.gov.in/IIB/sideMenuAction.do?method=doLogOut&loggedInUserId=26617