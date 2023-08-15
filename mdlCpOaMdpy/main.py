#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import mdlCpOaMdpy
import pandas as pd
import json

def customerByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    按照日期同步客户
    :param token:
    :param FDate:
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res=mdlCpOaMdpy.customerInterfaceByDate(option1=option, FVarDateTime=FDate, token_china=token, token_erp=key[0]["FApp2"])

    return res


def supplierByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    按照日期同步供应商
    :param token:
    :param FDate:
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res=mdlCpOaMdpy.supplierInterfaceByDate(option1=option, FVarDateTime=FDate, china_token=token, erp_token=key[0]["FApp2"])

    return res




