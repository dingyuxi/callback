# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 18:50:17 2018

@author: admin
"""

import sqlalchemy
import pandas as pd
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

#jiumiaodai = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rr-wz99yt7zx10n8f73e.mysql.rds.aliyuncs.com:3306/jiumiaodai?charset=utf8')
risk_analyse = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse?charset=utf8')

metedata = MetaData()

#user = Table('user', metadata, Column('id', Integer, primary_key=True),Column('name', String(50)), Column('fullname', String(50)), Column('password', String(12))

class user(Base):
    __tablename__ = 'test_user'
    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    password = Column(String(64))

Base.metedata.create_all(risk_analyse)

#    def __init__(self, name, fullname, password):
#        self.name = name
#        self.fullname = fullname
#        self.password = password
#mapper(User, user)




