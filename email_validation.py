#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Description
"""Truemail: Python3-Based Email Validator.""

# Standard library imports
import re
import os
import sys
import socks
import socket
import logging
import smtplib
import itertools
import dns.resolver
import pandas as pd

# External library imports
from time import sleep
from requests import get
from random import randint
from bs4 import BeautifulSoup
from selenium import webdriver

# Paths
CWD = str(os.path.abspath(os.path.dirname(__file__)) + '/')

# Records
DF = pd.read_excel(CWD + 'contact_master.xlsx', names=['email_ids'], header=0)
DF = df_format = DF['email_ids'].str.lower()

# Array declaration
valid = []
invalid = []
error_log = []
proxy_list = []
proxies = []


# Get proxies
def get_proxies():
    print('\033[1m' + 'Updating hosts…' + '\033[0;m')
    browser = webdriver.Chrome(executable_path=r'C:\Users\Bijan\Downloads\chromedriver_win32\chromedriver.exe')
    browser.get('https://hidemy.name/en/proxy-list/?type=4#list')
    sleep(randint(5, 6))
    html_source = browser.page_source
    browser.quit()
    soup = BeautifulSoup(html_source, 'html.parser')
    data = soup.select('tbody tr')
    for dat in data:
        try:
            proxy_list.append(dat.select('td')[0].text + ' ' + dat.select('td')[1].text)
        except Exception as e:
            logging.exception(e)
    print(proxy_list)


# Proxy tunnel
def proxy_chain():
    for proxy in proxy_list:
        try:
            socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS4, proxy.split()[0], int(proxy.split()[1]))
            socks.wrapmodule(smtplib)
            print('\033[92m' + '[200 OK]' + '\033[0;m', get('https://api.ipify.org/').text)
            proxies.append(proxy)
            pass
        except Exception as e:
            logging.exception(e)
            print('\033[91m' + '[ERR 54]' + '\033[0;m', proxy.split()[0])
            pass


# Syntax matching
def check_syntax():
    for email in DF:
        match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', email)
        if match:
            valid.append(email)
        else:
            invalid.append(email)
            error_log.append('Invalid Syntax')


# DNS resolution
def resolve_dns():
    for email in valid:
        try:
            dns.resolver.query(str(email.split('@')[1]), 'MX')
        except Exception as e:
            logging.exception(e)
            valid.remove(email)
            invalid.append(email)
            error_log.append('Unresolved DNS')
            pass


# SMTP verification
def verify_smtp():
    for email, proxy in zip(valid, itertools.cycle(proxies)):
        try:
            socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS4, proxy.split()[0], int(proxy.split()[1]))
            socks.wrapmodule(smtplib)
            print('Sending request via', get('https://api.ipify.org/').text)
            records = dns.resolver.query(str(email.split('@')[1]), 'MX')
            mx_records = str(records[0].exchange)
            host = socket.gethostname()
            server = smtplib.SMTP()
            server.set_debuglevel(-1)
            server.connect(mx_records)
            server.helo(host)
            server.mail('zula_haylom@aol.com')
            code, message = server.rcpt(str(email))
            server.quit()
            if code == 250:
                continue
        except Exception as e:
            logging.exception(e)
            valid.remove(email)
            invalid.append(email)
            error_log.append('No record found')


# Export validated CSV file
def export_csv():
    valid_length = '(' + str(len(valid)) + ')'
    invalid_length = '(' + str(len(invalid)) + ')'
    valid_emails = pd.Series(valid, name='Valid Emails ' + valid_length)
    invalid_emails = pd.Series(invalid, name='Invalid Emails ' + invalid_length)
    validation_check = pd.Series(error_log, name='Reason')
    concat_list = pd.concat([valid_emails, invalid_emails, validation_check], axis=1)
    records_validated = pd.DataFrame(concat_list)
    records_validated.to_csv('records_validated.csv', index=False)


get_proxies()
proxy_chain()
check_syntax()
resolve_dns()
verify_smtp()
export_csv()

sys.exit()
