#-*- coding:utf-8 -*-
import sqlalchemy
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 创建对象的基类:
Base = declarative_base()

# 定义User对象:
class User(Base):
    # 表的名字:
    __tablename__ = 'dyx_study_user'

    # 表的结构:
    id = Column(String(20), primary_key=True)
    name = Column(String(20))

# 初始化数据库连接:

# 创建DBSession类型:
risk_analyse = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse?charset=utf8')

#Base.metadata.create_all(risk_analyse)

DBSession = sessionmaker(bind=risk_analyse)
session = DBSession()

#new_user = User(id='5',name='Bob')

#session.add(new_user)

#session.commit()

#session.close()
my_user = session.query(User).filter(User.id=='5').one()
print(my_user)
