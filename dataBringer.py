import requests
import json
import pandas as pd
import holidays
import numpy as np
import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
import time
import data

def downloadDataForecast():
    
    chrome_options = Options()
    chrome_options.headless = False
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option("excludeSwitches",["enable-automation"])
    chrome_options.add_experimental_option("prefs", {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "download.default_directory": "C:\\Users\\omer.adiguzel\\Desktop\\autoMania\\",
            "download.prompt_for_download": False,
            'profile.default_content_setting_values.automatic_downloads': 1,
            "profile.password_manager_enabled": False
    })
    driver = webdriver.Chrome(service=ChromeService(executable_path='chromedriver.exe'), options=chrome_options)
    driver.get(data.url)
    driver.maximize_window()
    
    # print('Driver launched in silent mode.')
    print('Driver get {}'.format(data.url))

    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, data.xpath_login))).send_keys(data.user) # login
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, data.xpath_password))).send_keys(data.password) # password
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, data.xpath_submit))).click() # submit
    print('Logged into ********.')
    hour_should = data.hour_should    
    text_hour = ''
    
    while not (hour_should == text_hour[:5]):
        text_hour = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[4]/div/div/div[1]/div/xt-porlet-time-picker/div[1]/span"))).text
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[4]/div/div/div[1]/div/xt-porlet-time-picker/div[1]/nav/iron-icon[1]"))).click()
        print('Correcting hour. Hour is : {}'.format(text_hour))    

    time.sleep(5)
    # driver.execute_script("document.body.style.zoom='75%'")
    for pathes in [data.xpath_wind, data.xpath_hydro,data.xpath_sun,data.xpath_alis]:
        if pathes == data.xpath_sun:
            driver.execute_script("window.scrollTo(0, window.scrollY + 200)")
        WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, pathes ))).click() #threedot
        time.sleep(.5)
        WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/ul/li[12]"))).click() # export csv button
        time.sleep(.5)
        WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[5]/div/div[5]/span[2]"))).click() # confirm warning button
        time.sleep(.5)
        WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[5]/div/div[5]/span[1]"))).click() # interval confirm button
        print('File downloaded.')
        time.sleep(5)
# WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='taLnk ulBlueLinks']"))).click()
    time.sleep(5)
    driver.quit()
    # return 'Data from ***** downloaded.'


def getDataTrainFromEpias():

    payload_ticket = 'username=******&password=******'
    headers_ticket = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
                    }

    response_ticket = requests.request("POST", data.url_ticket, headers=headers_ticket, data=payload_ticket)

    tgt = response_ticket.json()['tgt']

    one_before = (datetime.datetime.today() - datetime.timedelta(days= 1)).strftime('%Y-%m-%d')
    payload = json.dumps({
    "endDate": "{}T00:00:00+03:00".format(one_before),
    "startDate": "2024-08-27T00:00:00+03:00"
    })

    headers = {
    'Content-Type': 'application/json',
    'tgt' : tgt
    }
    response_ptf = requests.request("POST", data.url_ptf, headers=headers, data=payload)
    print('Response from ptf', response_ptf.status_code)
    # response_prod = requests.request("POST", data.url_prod, headers=headers, data=payload)
    # print('Response from prod', response_prod.status_code)
    response_prod_missing = requests.request("POST", data.url_prod_missing, headers=headers, data=payload)
    print('Response from prod_missing', response_prod_missing.status_code)
    # response_alis = requests.request("POST", data.url_alis, headers=headers, data=payload)
    # print('Response from alis', response_alis.status_code)
    response_alis_missing = requests.request("POST", data.url_alis_missing, headers=headers, data=payload)
    print('Response from alis_missing', response_alis_missing.status_code)

    data_ptf = response_ptf.json()
    # data_prod = response_prod.json()
    data_prod_missing = response_prod_missing.json()
    # data_alis = response_alis.json()
    data_alis_missing = response_alis_missing.json()

    df_ptf = pd.DataFrame(data_ptf['items'])
    # df_prod = pd.DataFrame(data_prod['items'])
    df_prod_missing = pd.DataFrame(data_prod_missing['items'])
    # df_alis = pd.DataFrame(data_alis['items'])
    df_alis_missing = pd.DataFrame(data_alis_missing['items'])
    # df_alis = df_alis.rename(columns={'period' : 'date'})


    df_ptf = df_ptf[['date', 'price']]
    # df_prod = df_prod[['date','river', 'dam','wind','sun']]
    df_prod_missing = df_prod_missing[['date','river', 'dammedHydro','wind','sun']].rename(columns = {'dammedHydro' : 'dam'})
    # df_prod_missing = df_prod_missing[df_prod_missing['date'] > df_prod['date'].iloc[-1]].reset_index(drop = True)
    # df_alis_missing = df_alis_missing[df_alis_missing['date'] > df_alis['date'].iloc[-1]].reset_index(drop = True)
    df_ptf['date'] = pd.to_datetime(df_ptf['date'], infer_datetime_format=True) 
    df_ptf['date'] = df_ptf['date'].dt.tz_localize(None)
    # df_prod['date'] = pd.to_datetime(df_prod['date'], infer_datetime_format=True) 
    # df_prod['date'] = df_prod['date'].dt.tz_localize(None) 
    df_prod_missing['date'] = pd.to_datetime(df_prod_missing['date'], infer_datetime_format=True) 
    df_prod_missing['date'] = df_prod_missing['date'].dt.tz_localize(None)
    # df_alis['date'] = pd.to_datetime(df_alis['date'], infer_datetime_format=True) 
    # df_alis['date'] = df_alis['date'].dt.tz_localize(None) 
    df_alis_missing['date'] = pd.to_datetime(df_alis_missing['date'], infer_datetime_format=True) 
    df_alis_missing['date'] = df_alis_missing['date'].dt.tz_localize(None) 
    # df_prod_last = pd.concat([df_prod, df_prod_missing], ignore_index = True )
    df_prod_last = df_prod_missing.copy()
    # df_alis_last = pd.concat([df_alis, df_alis_missing.rename(columns= {'consumption' : 'swv'})], ignore_index = True )
    df_alis_last = df_alis_missing.rename(columns= {'consumption' : 'swv'})
    df = df_ptf.set_index('date').join(df_prod_last.set_index('date')).reset_index()
    df = df.rename(columns= {'date' : 'ds', 'price' : 'y'})
    df['alis'] = df_alis_last['swv']

    return df

 