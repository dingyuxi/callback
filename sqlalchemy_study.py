#from sshtunnel import SSHTunnelForwarder
import sqlalchemy
from sqlalchemy import Table, MetaData, Column, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, sessionmaker

engine = sqlalchemy.create_engine('mysql+pymysql://dingyuxi:IV3ETnP1i3@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse?charset=utf8')


metadata = MetaData()

user = Table('dyx_study_user', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)),
            #Column('fullname', String(50)),
            #Column('password', String(12))
        )

class User(object):
    #def __init__(self, name, fullname, password):
    def __init__(self, name, id):
        self.name = name
        self.id = id
        #self.fullname = fullname
        #self.password = password

mapper(User, user)

Session = sessionmaker(bind=engine)
session = Session()

#user_obj = User(name="dyx", id=12)
#print(user_obj.name, user_obj.fullname)

#session.add(user_obj)
#session.commit()
user = session.query(User).filter(User.id=='5').one()
print('name',user.name)
session.close()
 
