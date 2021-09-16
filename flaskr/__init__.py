# -*- coding: utf-8 -*-
#coding:utf8
from flask import Flask, jsonify, request, abort, make_response, url_for, redirect, render_template, session
import os
import json
import datetime
import csv
from .models import setup_db, Country, CurrencyDetails, FxRates, Users, Transactions, Fraudster
import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import uuid
import re


psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

#ETL APP
def create_app(test_config=None):
    app = Flask(__name__,instance_relative_config=True)
    SECRET_KEY = "cha-ZZ0_1n!@#$ge123me1"
    app.config.from_object(__name__)
    setup_db(app)

    excelFolder = os.path.join(app.static_folder, 'excel')
    excelFiles = ['countries.csv', 'currency_details.csv', 'fx_rates.csv', 'transactions.csv', 'users.csv']
    @app.route('/home1')
    def index1():
        countriesFile = os.path.join(excelFolder, 'countries.csv')
        currencyDetailsFile = os.path.join(excelFolder, 'currency_details.csv')
        fxRatesFile = os.path.join(excelFolder, 'fx_rates.csv')
        usersFile = os.path.join(excelFolder, 'users.csv')
        transactionsFile = os.path.join(excelFolder, 'transactions.csv')
        frudFile = os.path.join(excelFolder, 'fraudsters.csv')


        missingFiles = [{'Files_info':[]}]
        invalidRows = [{
                        'countries_ignored_rows':[],
                        'currencyDetails_ignored_rows':[],
                        'fxRates_ignored_rows':[],
                        'users_ignored_rows': [],
                        'transactions_ignored_rows': [],
                        'fraudsters_ignored_rows': [],
                        }]

        # anlysis success and fail
        CountryValidEntry = 0
        currencyDetailsValidEntry = 0
        fxRatesValidEntry = 0
        usersValidEntry = 0
        transactionsValidEntry = 0
        fraudstersValidEntry = 0
        CountryCompeletePrecent = 0
        currencyDetailsCompeletePrecent = 0
        fxRatesCompeletePrecent = 0
        usersCompeletePrecent = 0
        transactionsCompeletePrecent = 0
        fraudstersCompeletePrecent = 0
        compeletePrecentages = []
        # ------ Countries Start ------
        try:
            countriesData = pd.read_csv(countriesFile)
            countriesData = countriesData.fillna(np.nan).replace([np.nan], [None])
            len_countries_rows = countriesData.shape[0]
            countires_columns = countriesData.columns
            for country_index in range(len_countries_rows):
                country_obj = {'country_code': '', 'name': '', 'code3': '', 'numcode': None, 'phonecode': None}
                for country_column_head in countires_columns:
                    if country_column_head.lower() == "code":
                        country_obj['country_code'] = countriesData[country_column_head].loc[country_index]

                    if country_column_head.lower() == "name":
                        country_obj['name'] = countriesData[country_column_head].loc[country_index]

                    if country_column_head.lower() == "code3":
                        country_obj['code3'] = countriesData[country_column_head].loc[country_index]

                    if country_column_head.lower() == "numcode":
                        numcodeCheck = str(countriesData[country_column_head].loc[country_index]).split(".")[0]
                        is_numcode_number = numcodeCheck.isnumeric()
                        if is_numcode_number:
                            country_obj['numcode'] = int(countriesData[country_column_head].loc[country_index])
                        else:
                            country_obj['numcode'] = None

                    if country_column_head.lower() == "phonecode":
                        phonecode_check = str(countriesData[country_column_head].loc[country_index]).split(".")[0]
                        is_phonecode_number = phonecode_check.isnumeric()
                        if is_phonecode_number:
                            country_obj['phonecode'] = countriesData[country_column_head].loc[country_index]
                        else:
                            country_obj['phonecode'] = None
                try:
                    newCountry = Country(
                                    country_code= country_obj['country_code'],
                                    name= country_obj['name'],
                                    code3= country_obj['code3'],
                                    numcode=country_obj['numcode'],
                                    phonecode=country_obj['phonecode']
                                    )
                    newCountry.insert()
                    CountryValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} countries.csv not added \n".format(country_index+1)
                    invalidRows[0]['countries_ignored_rows'].append(invalidRowMsg)
            CountryCompeletePrecent = (CountryValidEntry / len_countries_rows) * 100
            compeletePrecentages.append({"countryfile_success": str(CountryCompeletePrecent) + "%"})
            allContries = [x.format() for x in Country.query.all()]
            missingFiles[0]['Files_info'].append("countries.csv Uploaded successfully")
        except:
            missingFiles[0]['Files_info'].append("Not Found countries.csv in excel folder")

        # ------ Countries End ------
        # ------ currency_details Start ------
        try:
            cd = pd.read_csv(currencyDetailsFile)
            cd = cd.fillna(np.nan).replace([np.nan], [None])
            len_cd_rows = cd.shape[0]
            cd_columns = cd.columns
            for cd_index in range(len_cd_rows):
                cd_obj = {'ccy': '', 'iso_code': None, 'exponent': None, 'is_crypto': False}
                for cd_column_head in cd_columns:
                    if cd_column_head.lower() == "currency":
                        cd_obj['ccy'] = cd[cd_column_head].loc[cd_index]
                    # handle incase the value not number 100%
                    if cd_column_head.lower() == "iso_code":
                        iso_code_check = str(cd[cd_column_head].loc[cd_index]).split(".")[0]
                        is_isocode_number = iso_code_check.isnumeric()
                        if is_isocode_number:
                            cd_obj['iso_code'] = int(cd[cd_column_head].loc[cd_index])
                        else:
                            cd_obj['iso_code'] = None

                    if cd_column_head.lower() == "exponent":

                        exponent_check = str(cd[cd_column_head].loc[cd_index]).split(".")[0]
                        is_exponent_number = exponent_check.isnumeric()
                        if is_exponent_number:
                            cd_obj['exponent'] = int(cd[cd_column_head].loc[cd_index])
                        else:
                            cd_obj['exponent'] = None

                    if cd_column_head.lower() == "is_crypto":
                        is_crypto_check = cd[cd_column_head].loc[cd_index]
                        # in case missing one value consider it False due to Nullable false
                        if is_crypto_check == None:
                            cd_obj['is_crypto'] = False
                        else:
                            cd_obj['is_crypto'] = cd[cd_column_head].loc[cd_index]

                try:
                    newCurrencyDetails = CurrencyDetails(
                                    ccy= cd_obj['ccy'],
                                    iso_code= cd_obj['iso_code'],
                                    exponent= cd_obj['exponent'],
                                    is_crypto=cd_obj['is_crypto'],
                                    )
                    newCurrencyDetails.insert()
                    currencyDetailsValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} currency_details.csv not added \n".format(cd_index+1)
                    invalidRows[0]['currencyDetails_ignored_rows'].append(invalidRowMsg)
            currencyDetailsCompeletePrecent = (currencyDetailsValidEntry / len_cd_rows) * 100
            compeletePrecentages.append({"currency_detailsfile_success": str(currencyDetailsCompeletePrecent) + "%"})
            missingFiles[0]['Files_info'].append("currency_details.csv Uploaded successfully")
        except:
            missingFiles[0]['Files_info'].append("Not Found currency_details.csv in excel folder")

        # --- currency_details end ----
        # ---- fx_rates start ----
        try:
            fxr = pd.read_csv(fxRatesFile)
            fxr = fxr.fillna(np.nan).replace([np.nan], [None])
            len_fxr_rows = fxr.shape[0]
            fxr_columns = fxr.columns
            for fxr_index in range(len_fxr_rows):
                fxr_obj = {'base_ccy': '', 'ccy': '', 'rate': None}

                for fxr_column_head in fxr_columns:
                    if fxr_column_head.lower() == "base_ccy":
                        fxr_obj['base_ccy'] = fxr[fxr_column_head].loc[fxr_index]

                    if fxr_column_head.lower() == "ccy":
                        fxr_obj['ccy'] = fxr[fxr_column_head].loc[fxr_index]

                    if fxr_column_head.lower() == "rate":
                        fxr_obj['rate'] = fxr[fxr_column_head].loc[fxr_index]
                try:
                    newFxRates = FxRates(base_ccy = fxr_obj['base_ccy'], ccy = fxr_obj['ccy'], rate = fxr_obj['rate'])
                    newFxRates.insert()
                    fxRatesValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} fx_rates.csv not added \n".format(fxr_index+1)
                    invalidRows[0]['fxRates_ignored_rows'].append(invalidRowMsg)

            fxRatesCompeletePrecent = (fxRatesValidEntry / len_fxr_rows) * 100
            compeletePrecentages.append({"fx_ratesfile_success": str(fxRatesCompeletePrecent) + "%"})
            missingFiles[0]['Files_info'].append("fx_rates.csv Uploaded successfully")
            allnewFxRates = [r.format() for r in FxRates.query.all()]
        except:
            missingFiles[0]['Files_info'].append("Not Found fx_rates.csv in excel folder")

        # --- fx_rates end ----
        # ---- users start ----

        try:
            users = pd.read_csv(usersFile)
            users = users.fillna(np.nan).replace([np.nan], [None])
            len_users_rows = users.shape[0]
            users_columns = users.columns

            for users_index in range(len_users_rows):
                user_obj = {
                            'user_index': None,
                            'id': '',
                            'has_email': False,
                            'phone_country': None,
                            'terms_version': None,
                            'created_date': datetime.datetime.now(tz=None).time(),
                            'state': '',
                            'country': '',
                            'birth_year': None,
                            'kyc': '',
                            'failed_sign_in_attempts': None
                            }
                i = 0
                for users_column_head in users_columns:
                    if i == 0:
                        i = 1
                        user_obj['user_index'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "failed_sign_in_attempts":
                        user_obj['failed_sign_in_attempts'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "kyc":
                        user_obj['kyc'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "birth_year":
                        birth_yearCheck = str(users[users_column_head].loc[users_index]).split(".")[0]
                        is_birth_year_number = birth_yearCheck.isnumeric()
                        if is_birth_year_number:
                            user_obj['birth_year'] = int(users[users_column_head].loc[users_index])
                        else:
                            user_obj['birth_year'] = None

                    if users_column_head.lower() == "country":
                        user_obj['country'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "state":
                        state_check = users[users_column_head].loc[users_index]
                        if state_check:
                            user_obj['state'] = users[users_column_head].loc[users_index]
                        else:
                            user_obj['state'] = ""

                    if users_column_head.lower() == "created_date":
                        created_date_check = users[users_column_head].loc[users_index]
                        if created_date_check:
                            user_obj['created_date'] = users[users_column_head].loc[users_index]
                        else:
                            user_obj['created_date'] = datetime.datetime.now(tz=None).time()
                    if users_column_head.lower() == "terms_version":
                        user_obj['terms_version'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "phone_country":
                        user_obj['phone_country'] = users[users_column_head].loc[users_index]

                    if users_column_head.lower() == "has_email":
                        has_email_check = users[users_column_head].loc[users_index]
                        if has_email_check:
                            user_obj['has_email'] = users[users_column_head].loc[users_index]
                        else:
                            user_obj['has_email'] = False

                    if users_column_head.lower() == "id":
                        user_obj['id'] = users[users_column_head].loc[users_index]
                try:
                    newUser = Users(
                                    id=user_obj['id'],
                                    has_email=user_obj['has_email'],
                                    phone_country=user_obj['phone_country'],
                                    terms_version=user_obj['terms_version'],
                                    created_date=user_obj['created_date'],
                                    state=user_obj['state'],
                                    country=user_obj['country'],
                                    birth_year=user_obj['birth_year'],
                                    kyc=user_obj['kyc'],
                                    failed_sign_in_attempts=user_obj['failed_sign_in_attempts'],
                                    user_index = user_obj['user_index']
                                    )
                    newUser.insert()
                    usersValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} users.csv not added \n".format(users_index+1)
                    invalidRows[0]['users_ignored_rows'].append(invalidRowMsg)

            usersCompeletePrecent = (usersValidEntry / len_users_rows) * 100
            compeletePrecentages.append({"usersfile_success": str(usersCompeletePrecent) + "%"})
            missingFiles[0]['Files_info'].append("users.csv Uploaded successfully")
        except:
            missingFiles[0]['Files_info'].append("Not Found users.csv in excel folder")

        # ---- users end ----
        # ---- Transactions Start ----
        problem_index = 0
        try:
            transactions = pd.read_csv(transactionsFile)
            transactions = transactions.fillna(np.nan).replace([np.nan], [None])
            len_transactions_rows = transactions.shape[0]
            transactions_columns = transactions.columns
            for transaction_index in range(len_transactions_rows):
                transaction_obj = {
                            'transaction_index': None,
                            'id': '',
                            'currency': None,
                            'amount': None,
                            'state': None,
                            'created_date': None,
                            'merchant_category': '',
                            'merchant_country': '',
                            'entry_method': None,
                            'user_id': None,
                            'type': None,
                            'source': None
                            }
                i2 = 0
                ignoreAdd = False
                for transaction_column_head in transactions_columns:
                    if i2 == 0:
                        i2 = 1
                        transaction_obj['transaction_index'] = transactions[transaction_column_head].loc[transaction_index]
                    if transaction_column_head.lower() == "currency":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['currency'] = None
                        else:
                            transaction_obj['currency'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "amount":
                        transaction_obj['amount'] = transactions[transaction_column_head].loc[transaction_index]

                    if transaction_column_head.lower() == "state":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['state'] = None
                        else:
                            transaction_obj['state'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "created_date":

                        transaction_obj['created_date'] = transactions[transaction_column_head].loc[transaction_index]

                    if transaction_column_head.lower() == "merchant_category":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['merchant_category'] = None
                        else:
                            transaction_obj['merchant_category'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    # my native fix regx
                    if transaction_column_head.lower() == "merchant_country":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['merchant_country'] = None
                        else:
                            # !!! my native fix regx !!! ----------------------
                            marchentCountry = transactions[transaction_column_head].loc[transaction_index]
                            if len(marchentCountry) > 3:
                                theCountry = re.search("[A-Z]{3}\Z", marchentCountry)
                                if theCountry:
                                    transaction_obj['merchant_country'] = theCountry.group()
                                    problem_index += 1
                                    print("solved problem" + str(problem_index))
                                else:
                                    validMarchent = marchentCountry.replace("%20","")
                                    validMarchent = validMarchent.strip()
                                    validMarchent = validMarchent.replace(' ', '')
                                    transaction_obj['merchant_country'] = validMarchent
                            else:
                                transaction_obj['merchant_country'] = transactions[transaction_column_head].loc[transaction_index]

                    if transaction_column_head.lower() == "entry_method":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['entry_method'] = None
                        else:
                            transaction_obj['entry_method'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "user_id":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['user_id'] = None
                        else:
                            transaction_obj['user_id'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "type":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['type'] = None
                        else:
                            transaction_obj['type'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "source":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['source'] = None
                        else:
                            transaction_obj['source'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                    if transaction_column_head.lower() == "id":
                        if transactions[transaction_column_head].loc[transaction_index] == None:
                            transaction_obj['id'] = None
                        else:
                            transaction_obj['id'] = transactions[transaction_column_head].loc[transaction_index].replace("%s", "")

                try:
                    newTransaction = Transactions(id=transaction_obj['id'],
                    currency=transaction_obj['currency'],
                    amount=transaction_obj['amount'],
                    state=transaction_obj['state'],
                    created_date=transaction_obj['created_date'],
                    merchant_category = transaction_obj['merchant_category'],
                    merchant_country = transaction_obj['merchant_country'],
                    entry_method=transaction_obj['entry_method'],
                    user_id=transaction_obj['user_id'],
                    type=transaction_obj['type'],
                    source = transaction_obj['source'],
                    transaction_index = transaction_obj['transaction_index']
                    )
                    newTransaction.insert()
                    transactionsValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} transactions.csv not added \n".format(transaction_index+1)
                    invalidRows[0]['transactions_ignored_rows'].append(invalidRowMsg)

            transactionsCompeletePrecent = (transactionsValidEntry / len_transactions_rows) * 100
            compeletePrecentages.append({"transactions": str(transactionsCompeletePrecent) + "%"})
            missingFiles[0]['Files_info'].append("transactions.csv Uploaded successfully")
        except:
            missingFiles[0]['Files_info'].append("Not Found transactions.csv in excel folder")

        # ---- Transactions end ----
        # ---- frud start ------
        try:
            fraudster = pd.read_csv(frudFile)
            fraudster = fraudster.fillna(np.nan).replace([np.nan], [None])
            len_fraudster_rows = fraudster.shape[0]
            fraudster_columns = fraudster.columns
            for fraudster_index in range(len_fraudster_rows):
                frudster_obj = {'user_id': '', 'frud_index': 0}
                i3 = 0
                for frudster_column_head in fraudster_columns:
                    if i3 == 0:
                        i3 = 1
                        frudster_obj['frud_index'] = fraudster[frudster_column_head].loc[fraudster_index]
                    if frudster_column_head.lower() == "user_id":
                        frudster_obj['user_id'] = fraudster[frudster_column_head].loc[fraudster_index]
                try:
                    newFrudSter = Fraudster(frud_index=frudster_obj['frud_index'], user_id=frudster_obj['user_id'])
                    newFrudSter.insert()
                    fraudstersValidEntry += 1
                except:
                    invalidRowMsg = "Invalid Data In Row Num {} fraudsters.csv not added \n".format(fraudster_index+1)
                    invalidRows[0]['fraudsters_ignored_rows'].append(invalidRowMsg)
            fraudstersCompeletePrecent = (fraudstersValidEntry / len_fraudster_rows) * 100
            compeletePrecentages.append({"fraudsters": str(fraudstersCompeletePrecent) + "%"})
            missingFiles[0]['Files_info'].append("fraudsters.csv Uploaded successfully")
        except:
            missingFiles[0]['Files_info'].append("Not Found fraudsters.csv in excel folder")

        # --- frud end ----
        return jsonify(invalidRows + missingFiles + compeletePrecentages)



    @app.route('/transactions_api')
    def transactionsAPI():
        allTransactions = []
        for transaction in Transactions.query.all():
            transaction_obj = {
                        'transaction_index': transaction.transaction_index,
                        'id': transaction.id,
                        'currency': transaction.currency,
                        'amount': transaction.amount,
                        'state': transaction.state,
                        'created_date': transaction.created_date,
                        'merchant_category': transaction.merchant_category,
                        'merchant_country': transaction.merchant_country,
                        'entry_method': transaction.entry_method,
                        'user_id': transaction.user_id,
                        'type': transaction.type,
                        'source': transaction.source
                        }
            allTransactions.append(transaction_obj)
        return jsonify(allTransactions)

    @app.route('/users')
    def users_api():
        allusers = []
        for user in Users.query.all():
            user_obj = {
                    'user_index': user.user_index,
                    'id': user.id,
                    'has_email': user.has_email,
                    'phone_country': user.phone_country,
                    'terms_version': user.terms_version,
                    'created_date': str(user.created_date),
                    'state': user.state,
                    'country': user.country,
                    'birth_year': user.birth_year,
                    'kyc': user.kyc,
                    'failed_sign_in_attempts': user.failed_sign_in_attempts
                    }
            allusers.append(user_obj)
        return jsonify(allusers)

    ## --- API Routes ---- ##

    @app.route('/countries_api')
    def countriesApi():
        allContries = []
        for country in Country.query.all():
            country_obj = {'country_code': country.country_code, 'name': country.name, 'code3': country.code3, 'numcode': country.numcode, 'phonecode': country.phonecode}
            allContries.append(country_obj)
        return jsonify(allContries)


    @app.route('/fx_rates_api')
    def fxRateApi():
        allfxRates = []
        for fxrate in FxRates.query.all():
            fx_rate = {'base_ccy': fxrate.base_ccy, 'ccy': fxrate.ccy, 'rate': fxrate.rate}
            allfxRates.append(fx_rate)
        return jsonify(allfxRates)

    @app.route('/currency_details_api')
    def currencyDetailsAPi():
        allCurrencyDetails = []
        for currencyDetail in CurrencyDetails.query.all():
            currency_detail = {'ccy': currencyDetail.ccy, 'iso_code': currencyDetail.iso_code, 'exponent': currencyDetail.exponent, 'is_crypto': currencyDetail.is_crypto}
            allCurrencyDetails.append(currency_detail)
        return jsonify(allCurrencyDetails)


    @app.route('/fraudster')
    def fraudsterAPI():
        allfraudster = []
        for fraudster in Fraudster.query.all():
            fraudster_ob = frudster_obj = {'user_id': fraudster.user_id, 'frud_index': fraudster.frud_index}
            allfraudster.append(fraudster_ob)
        return jsonify(allfraudster)

    return app
