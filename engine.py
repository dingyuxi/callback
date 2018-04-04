#!/usr/local/lib/python
# -*- coding: utf-8 -*- 
"""
Created on Wed Apr 4 10:15:00 2018

@author: dyx
"""
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, BigInteger, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, sessionmaker

jiumiaodai_engine = sqlalchemy.create_engine("mysql+pymysql://dingyuxi:dingyuxi@2018@rr-wz99yt7zx10n8f73e.mysql.rds.aliyuncs.com/jiumiaodai?charset=utf8")
risk_analyse_engine = sqlalchemy.create_engine("mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com/risk_analyse?charset=utf8")
#risk_analyse_engine = create_engine("mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com/risk_analyse?charset=utf8")



jiumiaodai_base = declarative_base(jiumiaodai_engine)
risk_analyse_base = declarative_base(risk_analyse_engine)

jiumiaodai_session = sessionmaker(bind=jiumiaodai_engine)()
risk_analyse_session = sessionmaker(bind=risk_analyse_engine)()

#class Risk_Analyse(risk_analyse_base):
#    __abstract__ = True
#    __table_args__ = {
#        'autoload': True,
#    }


#class Dyx_Temp(Risk_Analyse):
#    __tablename__ = 'dyx_temp'




#data = risk_analyse_session.query(Dyx_Temp).filter(Dyx_Temp.order_id=='1831125').all()
#print(data[0].user_id)
