import os
import json
import datetime
import uuid
from sqlalchemy import Column, String, Integer, create_engine, ForeignKey, Boolean, Date, DateTime, Time, CHAR, BIGINT
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import *
database_name = "bookshelf"
database_path = "postgres://{}:{}@{}/{}".format('student', 'student','localhost:5432', database_name)

db = SQLAlchemy()

'''
setup_db(app)
    binds a flask application and a SQLAlchemy service
'''
def setup_db(app, database_path=database_path):
    app.config["SQLALCHEMY_DATABASE_URI"] = database_path
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.app = app
    db.init_app(app)
    db.drop_all()
    db.create_all()


class Transactions(db.Model):
  __tablename__ = 'transactions'
  id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
  currency = Column(CHAR(3), nullable=False)
  amount = Column(BIGINT, nullable=False)
  state = Column(VARCHAR(25), nullable=False)
  created_date = Column(Time(timezone=False), nullable=False, default=datetime.datetime.now(tz=None).time())
  merchant_category = Column(VARCHAR(100), nullable=True)
  merchant_country = Column(VARCHAR(3), nullable=True)
  entry_method = Column(VARCHAR(4), nullable=False)
  user_id = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4)
  type = Column(VARCHAR(20), nullable=False)
  source = Column(VARCHAR(20), nullable=False)
  transaction_index = Column(Integer, nullable=True)


  def __init__(self, id, currency, amount, state, created_date, merchant_category, merchant_country, entry_method, user_id, type, source, transaction_index):
    self.id = id
    self.currency = currency
    self.amount = amount
    self.state = state
    self.created_date = created_date
    self.merchant_category = merchant_category
    self.merchant_country = merchant_country
    self.entry_method = entry_method
    self.user_id = user_id
    self.type = type
    self.source = source
    self.transaction_index = transaction_index

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'id': self.id,
      'currency': self.currency,
      'amount': self.amount,
      'state': self.state,
      'created_date': self.created_date,
      'merchant_category': self.merchant_category,
      'merchant_country': self.merchant_country,
      'entry_method': self.entry_method,
      'user_id': self.user_id,
      'type': self.type,
      'source': self.source,
      'transaction_index': self.transaction_index,
    }


class Users(db.Model):
  __tablename__ = 'users'
  id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
  user_index = Column(Integer, nullable=True)
  has_email = Column(Boolean, nullable=False)
  phone_country = Column(VARCHAR(300), nullable=True)
  terms_version = Column(Date, nullable=True)
  created_date = Column(Time(timezone=False), nullable=False, default=datetime.datetime.now(tz=None).time())
  state = Column(VARCHAR(25), nullable=False)
  country = Column(VARCHAR(2), nullable=True)
  birth_year = Column(Integer, nullable=True)
  kyc = Column(VARCHAR(20), nullable=True)
  failed_sign_in_attempts = Column(Integer, nullable=True)


  def __init__(self, id, has_email, phone_country, terms_version, created_date, state, country, birth_year, kyc, failed_sign_in_attempts,user_index):
    self.id = id
    self.has_email = has_email
    self.phone_country = phone_country
    self.terms_version = terms_version
    self.created_date = created_date
    self.state = state
    self.country = country
    self.birth_year = birth_year
    self.kyc = kyc
    self.failed_sign_in_attempts = failed_sign_in_attempts
    self.user_index = user_index

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'user_index': self.user_index,
      'id': self.id,
      'has_email': self.has_email,
      'phone_country': self.phone_country,
      'terms_version': self.terms_version,
      'created_date': self.created_date,
      'state': self.state,
      'country': self.country,
      'birth_year': self.birth_year,
      'kyc': self.kyc,
      'failed_sign_in_attempts': self.failed_sign_in_attempts,
    }


class FxRates(db.Model):
  __tablename__ = 'fx_rates'
  rate_id = Column(Integer, primary_key=True)
  base_ccy = Column(VARCHAR(3), nullable=True)
  ccy = Column(VARCHAR(10), nullable=True)
  rate = Column(DOUBLE_PRECISION, nullable=True)


  def __init__(self, base_ccy, ccy, rate):
    self.base_ccy = base_ccy
    self.ccy = ccy
    self.rate = rate

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'rate_id': self.rate_id,
      'base_ccy': self.base_ccy,
      'ccy': self.ccy,
      'rate': self.rate,
    }


class CurrencyDetails(db.Model):
  __tablename__ = 'currency_details'

  ccy = Column(VARCHAR(10), primary_key=True)
  iso_code = Column(Integer, nullable=True)
  exponent = Column(Integer, nullable=True)
  is_crypto = Column(Boolean, nullable=False, default=False)


  def __init__(self, ccy, iso_code, exponent, is_crypto):
    self.ccy = ccy
    self.iso_code = iso_code
    self.exponent = exponent
    self.is_crypto = is_crypto

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'ccy': self.ccy,
      'iso_code': self.iso_code,
      'exponent': self.exponent,
      'is_crypto': self.is_crypto,
    }




class Country(db.Model):
  __tablename__ = 'country'

  country_id = Column(Integer, primary_key=True)
  country_code = Column(VARCHAR(2))
  name = Column(VARCHAR(50), nullable=False)
  code3 = Column(VARCHAR(3), nullable=True)
  numcode = Column(Integer, nullable=True)
  phonecode = Column(Integer, nullable=True)

  def __init__(self, country_code, name, code3, numcode, phonecode):
    self.country_code = country_code
    self.name = name
    self.code3 = code3
    self.numcode = numcode
    self.phonecode = phonecode

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'country_id': self.country_id,
      'country_code': self.country_code,
      'name': self.name,
      'code3': self.code3,
      'numcode': self.numcode,
      'phonecode': self.phonecode,
    }


class Fraudster(db.Model):
  __tablename__ = 'fraudster'

  user_id = Column(UUID(as_uuid=True), default=uuid.uuid4)
  frud_index = Column(Integer, nullable=True)
  frud_id = Column(Integer, primary_key=True, nullable=True)

  def __init__(self, user_id, frud_index):
    self.user_id = user_id
    self.frud_index = frud_index

  def insert(self):
    db.session.add(self)
    db.session.commit()

  def update(self):
    db.session.commit()

  def delete(self):
    db.session.delete(self)
    db.session.commit()

  def format(self):
    return {
      'user_id': self.user_id,
      'frud_index': self.frud_index,
    }
