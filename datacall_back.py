#!/usr/local/lib/python
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 14:54:42 2018

@author: dyx
"""
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, BigInteger, String, ForeignKey
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

risk_data_engine = create_engine("mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9m0197u650l4w0z.mysql.rds.aliyuncs.com/risk_data?charset=utf8")
risk_analyse_engine = create_engine("mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com/risk_analyse?charset=utf8")
#user_engine = create_engine("mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com/jiumiaodai?charset=utf8")


risk_data_base = declarative_base(risk_data_engine)
risk_ana_base = declarative_base(risk_analyse_engine )
#user_base = declarative_base(user_engine )

session_rd = sessionmaker(bind=risk_data_engine)()
session_ra = sessionmaker(bind=risk_analyse_engine)()
#session_us = sessionmaker(bind=user_base )()


class Risk_dataModel(risk_data_base):
    __abstract__ = True
    __table_args__ = {
        'autoload': True,
    }


class RcApiResult(Risk_dataModel):
    __tablename__ = 'rc_api_result'


class Risk_Analyse_M(risk_ana_base):
    __abstract__ = True
    __table_args__ = {
         'autoload': True,
    }


class Dyx_Temp(Risk_Analyse_M):
    __tablename__ = 'dyx_temp'


class User_Model(risk_ana_base):
    __abstract__ = True
    __table_args__ = {
        'autoload': True,
    }


class User(User_Model):
    __tablename__ = 'dyx_temp_user'


#data = session_rd.query(RcApiResult).filter(RcApiResult.category=='4').filter(RcApiResult.created_dt>='2018-01-01 00:00:00').all()
def func(user):
    if user.module == 'lr_model':
        if user.client_id == 'ios':
            if user.user_type == 1:
                if user.score >= 30:
                    return user.user_id
            else:
                if user.score >= 50:
                    return user.user_id
        else:
            if user.user_type == 1:
                if user.score >= 35:
                    return user.user_id
            else:
                if user.score >= 55:
                    return user.user_id
    elif user.module == 'lr_model2':
        if user.client_id == 'ios':
            if user.score >= 50:
                return user.user_id
        else:
            if user.score >= 55:
                return user.user_id
    elif user.module == 'lr_model3':
        if user.client_id == 'ios':
            if user.score >= 50:
                return user.user_id
        else:
            if user.score >= 55:
                return user.user_id
    elif user.module == 'lr_module4':
        if user.score >= 70:
            return user.user_id

if __name__ == '__main__':
    data = session_rd.query(RcApiResult).filter(RcApiResult.category=='4').filter(RcApiResult.created_dt>='2018-03-01 00:00:00').all()
    basicuser = session_ra.query(Dyx_Temp).filter((Dyx_Temp.rong360_bank_card+Dyx_Temp.rong360_bank_card_debit+Dyx_Temp.rong360_provident_fund+Dyx_Temp.rong360_social_security)<=3).group_by(Dyx_Temp.order_id).all()
    users = []
    for i in range(len(data)):
        result = func(data[i])
        if result is not None:
            users.append(result)
    basicusers = []
    for i in range(len(basicuser)):
        temp = basicuser[i].user_id
        basicusers.append(temp)
    result_user = list(set(users).intersection(set(basicusers)))
  
    tele_and_user = session_us.query(User.user_id, User.telephone).filter(User.user_id in result_user).all()
    
#    path = '/home/dingyuxi/user.txt'
#    fileopen = open(path,'w')
#    for user in result_user:
#        fileopen.write(str(user)+"\n")
#    fileopen.close()
#    data = session_ra.query(Dyx_Temp).filter(Dyx_Temp.order_id=='1831125').all()
#    print(data)
#    list1 = [1,2,3,444,555]
    for userid in result_user:
        user_obj = User(user_id = userid)
        session_ra.add(user_obj)
    session_ra.commit()
#    print(len(users))
#    print(len(basicusers))
#    print(len(users))
#    print(len(data))
#    print(len(basicuser))
#    print(basicuser[0].user_id)
#print(help(data))

