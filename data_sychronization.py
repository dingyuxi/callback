# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 11:10:30 2018

@author: admin
"""

import sqlalchemy
import pandas as pd

jiumiaodai = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rr-wz99yt7zx10n8f73e.mysql.rds.aliyuncs.com:3306/jiumiaodai?charset=utf8')
risk_analyse = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse?charset=utf8')

def user():
    query='''
    SELECT *
    FROM `user` 
    WHERE created_time>=UNIX_TIMESTAMP('2018-03-07')*1000
    '''
    data = pd.read_sql(query,jiumiaodai)
    return data

def contract():
    query='''
    SELECT *
    FROM contract
    WHERE created_time>=UNIX_TIMESTAMP('2018-03-07')*1000
    '''
    data = pd.read_sql(query,jiumiaodai)
    return data

def order():
    query='''
    SELECT *
    FROM `order`
    WHERE created_time>=UNIX_TIMESTAMP('2018-03-07')*1000
    '''
    data = pd.read_sql(query,jiumiaodai)
    return data
    
def to_user():
    query= 'truncate table dyx_user'
    risk_analyse.execute(query)
    data = user()
    data.to_sql('dyx_user',risk_analyse,schema='risk_analyse',if_exists='append',index=False)

def to_contract():
    query= 'truncate table dyx_contract'
    risk_analyse.execute(query)
    data = contract()
    data.to_sql('dyx_contract',risk_analyse,schema='risk_analyse',if_exists='append',index=False)
    
def to_order():
    query= 'truncate table dyx_order'
    risk_analyse.execute(query)
    data = order()
    data.to_sql('dyx_order',risk_analyse,schema='risk_analyse',if_exists='append',index=False)


to_user()
to_contract()
to_order()
