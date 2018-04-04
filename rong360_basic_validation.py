# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 11:10:30 2018

@author: admin
"""

import sqlalchemy
import pandas as pd

jiumiaodai = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rr-wz99yt7zx10n8f73e.mysql.rds.aliyuncs.com:3306/jiumiaodai?charset=utf8')
risk_control = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9m0197u650l4w0z.mysql.rds.aliyuncs.com:3306/risk_control?charset=utf8') 
risk_analyse = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse?charset=utf8') 

def user_basic_validation():
    query='''
    SELECT
    u.user_id
    ,u.telephone
    ,CASE WHEN u.id_card_passed_time>0 then 1 ELSE 0 END as id_card_passed_time
    ,CASE WHEN t1.user_id is NOT NULL THEN 1 ELSE 0 END idCardBack
    ,CASE WHEN t2.user_id is NOT NULL THEN 1 ELSE 0 END handIdCardPhoto
    ,CASE WHEN u.faces_passed_time>0 then   1 ELSE 0 END as faces_passed_time
    ,CASE WHEN u.bank_card_no<>'' then  1 ELSE 0 END as bank_card_no
    
    ,CASE WHEN t3.user_id is not null THEN 1 ELSE 0 END as expired_time
    ,CASE WHEN u.telephone_history_time>0 THEN 1 ELSE 0 END as telephone_history_time
    ,u.created_time
    ,u.client_id
    FROM `user` u
    LEFT JOIN (SELECT uf.user_id,uf.created_time from    user_face uf WHERE uf.type='idCardBack' AND uf.created_time>0 )t1
    on t1.user_id=u.user_id
    LEFT JOIN (SELECT uf.user_id,uf.created_time from    user_face uf WHERE uf.type='handIdCardPhoto' AND uf.created_time>0 )t2
    on t2.user_id=u.user_id
    LEFT JOIN (SELECT ui.user_id,ui.expired_time FROM user_info ui WHERE ui.expired_time>ui.created_time)t3
    on t3.user_id=u.user_id
    WHERE u.user_id NOT in (SELECT user_id FROM test_user)
    AND u.created_time>=UNIX_TIMESTAMP('2018-01-01')*1000
    '''
    data = pd.read_sql(query,jiumiaodai)
    return data

def rong360():
    query='''
    SELECT 
    DISTINCT 
    u.user_id 
    ,CASE WHEN r1.user_id is not  NULL and  LEFT(r1.created_time,13)>=UNIX_TIMESTAMP('2018-01-01')*1000 THEN 1 ELSE 0 END as bank_card
    ,CASE WHEN r2.user_id is not  NULL and  LEFT(r2.created_time,13)>=UNIX_TIMESTAMP('2018-01-01')*1000 THEN 1 ELSE 0 END as bank_card_debit
    ,CASE WHEN r3.user_id is not  NULL and  LEFT(r3.created_time,13)>=UNIX_TIMESTAMP('2018-01-01')*1000 THEN 1 ELSE 0 END as provident_fund
    ,CASE WHEN r4.user_id is not  NULL and  LEFT(r4.created_time,13)>=UNIX_TIMESTAMP('2018-01-01')*1000 THEN 1 ELSE 0 END as social_security
    ,u.created_time
    ,u.client_id
    FROM risk_control.`user` u
    LEFT JOIN rong360_bank_card r1
    ON r1.user_id=u.user_id  
    LEFT JOIN rong360_bank_card_debit r2
    ON r2.user_id=u.user_id 
    LEFT JOIN rong360_provident_fund r3
    ON r3.user_id=u.user_id 
    LEFT JOIN rong360_social_security r4
    ON r4.user_id=u.user_id 
    WHERE u.created_time>=UNIX_TIMESTAMP('2018-01-01')*1000
    AND u.user_id not in (SELECT user_id FROM test_user)
    '''
    data = pd.read_sql(query,risk_control)
    return data

def to_basic_validation():
    query = 'truncate table dyx_basic_validation'
    risk_analyse.execute(query)
    data = user_basic_validation()
    data.to_sql('dyx_basic_validation',risk_analyse,schema='risk_analyse',if_exists='append',index=False)
    
def to_rong360():
    query = 'TRUNCATE TABLE dyx_rong360'
    risk_analyse.execute(query)
    data = rong360()
    data.to_sql('dyx_rong360',risk_analyse,schema='risk_analyse',if_exists='append',index=False)
    
to_basic_validation()
to_rong360()
