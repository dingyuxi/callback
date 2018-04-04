#!/usr/local/lib/python
# -*- coding: utf-8 -*- 
"""
Created on Wed Apr 4 10:15:00 2018

@author: dyx
"""
import engine
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import Table, MetaData
from sqlalchemy import Table, MetaData, Column, BigInteger, String, ForeignKey


class Risk_Analyse(engine.risk_analyse_base):
    __abstract__ = True
    __table_args__ = {
        'autoload': True,
    }


class Dyx_Temp(Risk_Analyse):
    __tablename__ = 'dyx_temp'


class Jiumiaodai(engine.jiumiaodai_base):
    __abstract__ = True
    __table_args__ = {
        'autoload': True,
    }


class User(Jiumiaodai):
    __tablename__ = 'user'

class User_Info(Jiumiaodai):
    __tablename__ = 'user_info'

class User_Face(Jiumiaodai):
    __tablename__ = 'user_face'

if __name__ == '__main__':
#    data = engine.risk_analyse_session.query(Dyx_Temp).filter(Dyx_Temp.order_id=='1831125').all()
#    print(data[0].user_id)
    user_result = engine.jiumiaodai_session.query(User).filter(User.created_time>="unix_timestamp('2018-03-01')*1000").all()
    print(len(user_result))
