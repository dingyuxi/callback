# coding: utf-8

import json
import traceback

import web
web.config.debug = False
from sqlalchemy import Column, Integer, String, DateTime, text, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from user import User
from feature import property_functions
from model import Model
from utils.time_utils import datetime2timestamp
from utils.json_utils import json_default

Base = declarative_base()
risk_control = create_engine('mysql+pymysql://user_rc:K4ugIwIZP4@rm-wz9m0197u650l4w0z.mysql.rds.aliyuncs.com:3306/risk_control?charset=utf8') 
risk_data = create_engine('mysql+pymysql://user_rc:K4ugIwIZP4@rm-wz9m0197u650l4w0z.mysql.rds.aliyuncs.com:3306/risk_data?charset=utf8') 
risk_analyse = create_engine('mysql+pymysql://wangyihui:G1goewBjel@rm-wz9fru2w0p979j3x3.mysql.rds.aliyuncs.com:3306/risk_analyse')
DBSession = sessionmaker(bind=risk_analyse)
session = DBSession()

class Kuanbiao0315(Base):
    __tablename__ = 'wyh_kuan0315'

    id = Column(Integer, primary_key=True)
    created_dt = Column(DateTime, server_default=\
        text('CURRENT_TIMESTAMP'))
    user_id = Column(Integer)
    hour = Column(String(255))
    platform = Column(String(255))
    age = Column(String(255))
    race = Column(String(255))
    gender = Column(String(255))
    area = Column(String(255))
    bank_name = Column(String(255))
    zmxy_score = Column(String(255))
    salary = Column(String(255))
    social_identity = Column(String(255))
    company_type = Column(String(255))
    work_time = Column(String(255))
    education_level = Column(String(255))
    marital_status = Column(String(255))
    xbehavior_risk_score = Column(String(255))
    city_rate30 = Column(String(255))
    city_level = Column(String(255))
    credit_score = Column(String(255))
    valid_mobile_cnt = Column(String(255))
    whitename_score = Column(String(255))
    is_whitelist2 = Column(String(255))
    app_quantity = Column(String(255))
    relation = Column(String(255))
    phone_remain = Column(String(255))
    reg_days = Column(String(255))
    contact_cnt = Column(String(255))
    called_time = Column(String(255))
    sms_cnt = Column(String(255))
    callactivearea_calltimes = Column(String(255))
    callactivearea_call_prop = Column(String(255))
    contactsactivearea_call_prop = Column(String(255))
    callmobilebelongmatch = Column(String(255))
    callcontactsareamatch = Column(String(255))
    contactssize = Column(String(255))
    each_phone_cnt = Column(String(255))
    each_phone_cnt_prop = Column(String(255))
    nch_exceed15_cnt = Column(String(255))
    not_cas_dcnt = Column(String(255))
    cont3d_not_cas_cnt = Column(String(255))
    max_cont_not_cas_dcnt = Column(String(255))
    numberusedlong = Column(String(255))
    openduration = Column(String(255))
    dialin_callcount = Column(String(255))
    dialout_mobilecount = Column(String(255))
    dialin_callduration = Column(String(255))
    dialout_callduration = Column(String(255))
    receivemsgcount = Column(String(255))
    sendmsgcount = Column(String(255))
    mnopayfeescount = Column(String(255))
    mnosinglepaymentmax = Column(String(255))
    nightcallcnt = Column(String(255))
    nightcallduration_dialin = Column(String(255))
    nightcallcnt_dialin = Column(String(255))
    nightcallduration_dialout = Column(String(255))
    connectinfo110 = Column(String(255))
    connectinfo120 = Column(String(255))

    overdue_1 = Column(Integer)
    overdue_3 = Column(Integer)
    overdue_7 = Column(Integer)


def create_db():
    Base.metadata.create_all(risk_analyse)

def drop_db():
    Base.metadata.drop_all(risk_analyse)

def feature_process(f):
    if f['salary'] == u'2000元以下':
        f['salary'] = 1
    elif f['salary'] == u'2000-3000元':
        f['salary'] = 2
    elif f['salary'] == u'3001-5000元':
        f['salary'] = 3
    elif f['salary'] == u'5001-8000元':
        f['salary'] = 4
    elif f['salary'] == u'8001-12000元':
        f['salary'] = 5
    elif f['salary'] == u'12000元以上':
        f['salary'] = 6
    else:
        f['salary'] = 0

    if f['relation'] in (u'父亲', u'母亲'):
        f['relation'] = 1
    elif f['relation'] == u'配偶':
        f['relation'] = 2
    elif f['relation'] in (u'兄弟', u'姐妹'):
        f['relation'] = 3
    elif f['relation'] == u'子女':
        f['relation'] = 4
    else:
        f['relation'] = 0

    if f['gender'] == 0:
        f['gender'] = 'F'
    else:
        f['gender'] = 'M'

    if f['race'] == u'汉':
        f['race'] = 1
    elif f['race'] == '':
        f['race'] = 0
    else:
        f['race'] = 2

    if f['bank_name'] is None:
        f['bank_name'] = 0
    elif f['bank_name'] in (u'中国建设银行', u'中国工商银行', u'中国农业银行', u'中国银行'):
        f['bank_name'] = 1
    else:
        f['bank_name'] = 2

    if f['marital_status'] == u'已婚有子女':
        f['marital_status'] = 1
    elif f['marital_status'] == u'已婚无子女':
        f['marital_status'] = 2
    elif f['marital_status'] == u'未婚':
        f['marital_status'] = 3
    elif f['marital_status'] == u'离异':
        f['marital_status'] = 4
    elif f['marital_status'] == u'丧偶':
        f['marital_status'] = 5
    else:
        f['marital_status'] = 0

    if f['social_identity'] == u'工薪族':
        f['social_identity'] = 1
    elif f['social_identity'] == u'自由职业':
        f['social_identity'] = 2
    elif f['social_identity'] == u'企业主':
        f['social_identity'] = 3
    else:
        f['social_identity'] = 0
    
    if not f['reg_days']:
        f['reg_days'] = 0
    elif f['reg_days'] < 30:
        f['reg_days'] = 1
    elif f['reg_days'] < 90:
        f['reg_days'] = 2
    elif f['reg_days'] < 180:
        f['reg_days'] = 3
    elif f['reg_days'] < 360:
        f['reg_days'] = 4
    elif f['reg_days'] < 1080:
        f['reg_days'] = 5
    elif f['reg_days'] < 1800:
        f['reg_days'] = 6
    elif f['reg_days'] < 3600:
        f['reg_days'] = 7
    else:
        f['reg_days'] = 8

    if f['education_level'] == u'高中以下':
        f['education_level'] = 1
    elif f['education_level'] == u'高中':
        f['education_level'] = 2
    elif f['education_level'] == u'中专':
        f['education_level'] = 3
    elif f['education_level'] == u'大专':
        f['education_level'] = 4
    elif f['education_level'] in (u'本科', u'硕士', u'博士'):
        f['education_level'] = 5
    else:
        f['education_level'] = 0

    if f['is_whitelist2'] is None:
        f['is_whitelist2'] = 0

    if f['valid_mobile_cnt'] is None:
        f['valid_mobile_cnt'] = 0

    if f['credit_score'] is None:
        f['credit_score'] = 0

    if f['platform'] == 'ios':
        f['platform'] = 'apple'

    if f['zmxy_score'] is None:
        f['zmxy_score'] = 0

    if f['callcontactsareamatch'] == u'否':
        f['callcontactsareamatch'] = 0
    elif f['callcontactsareamatch'] == u'是':
        f['callcontactsareamatch'] = 1

    if f['callmobilebelongmatch'] == u'否':
        f['callmobilebelongmatch'] = 0
    elif f['callmobilebelongmatch'] == u'是':
        f['callmobilebelongmatch'] = 1

def main():
    t = datetime2timestamp('2018-01-01 00:00:00')
    res = risk_data.execute(
        'select * from rc_api_result where created_time >= %d' % t).fetchall()
    for u in res:
        try:
            # user_entity = risk_control.execute(
            #     'select level from user where user_id=%d' % u.user_id).fetchone()

            overdue_entity = risk_analyse.execute(
                'select * from zq_overdue_detail where user_id=%d' % u.user_id).fetchone()
            if not overdue_entity:
                continue

            user = User(u.user_id, u.order_id)

            if user.old_user_info.user_type == 1:
                continue

            feature_pool = {}
            required_features = [i for i in dir(Kuanbiao0315) if not i.startswith('_')]
            required_features.remove('id')
            required_features.remove('created_dt')
            required_features.remove('metadata')
            required_features.remove('overdue_1')
            required_features.remove('overdue_3')
            required_features.remove('overdue_7')
            Model.construct_features(user, required_features, feature_pool)

            feature_process(feature_pool)

            feature_pool.update({
                'overdue_1': 1 if overdue_entity.overdue_1 else 0,
                'overdue_3': 1 if overdue_entity.overdue_3 else 0,
                'overdue_7': 1 if overdue_entity.overdue_7 else 0,
            })
            # print json.dumps(feature_pool, ensure_ascii=False, indent=4, default=json_default)
            k = Kuanbiao0315(**feature_pool)
            session.add(k)
            session.commit()
        except:
            traceback.print_exc()

    session.close()

if __name__ == '__main__':
    drop_db()
    create_db()
    main()

