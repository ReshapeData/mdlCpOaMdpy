#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import hashlib
import time
from urllib import parse
import json
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from pyrda.dbms.rds import RdClient
import datetime

def insertData(app2,sql):
    '''
    将从OA接口里面获得的数据插入到对应的数据表中
    :param app2: 执行sql语句对象
    :param sql:  sql语句
    :return:
    '''
    app2.insert(sql)


def getData(app2,sql):
    '''
    从数据表中获得数据
    :param app2: 执行sql语句对象
    :param sql: sql语句
    :return: 返回查询到的数据
    '''
    result=app2.select(sql)

    return result


def getcode(app2,param,tableName,param1,param2):
    '''
    通过传入的参数获得相应的编码
    :param app2 执行sql语句对象:
    :param param 需要查询的字段:
    :param tableName 表名:
    :param param1 查询条件字段:
    :param param2 条件:
    :return 将查询的到的结果返回:
    '''
    if param2=="销售四部":
        param2="国际销售部"

    sql=f"select {param} from {tableName} where {param1}='{param2}'"

    res=app2.select(sql)

    return res[0][f'{param}']

def getSullierTypeCode(param):
    '''
    转换code码
    :param param: 条件
    :return:
    '''
    d={"采购":"CG","委外":"WW","服务":"FW","综合":"ZH"}

    res=d[param]

    return res

def getFinterId(app2,tableName):
    '''
    在两张表中找到最后一列数据的索引值
    :param app2: sql语句执行对象
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''

    sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"

    res = app2.select(sql)

    return res[0]['FMaxId']

def DetailDateIsExist (app2,FNumber,FNumber_value,tableName):
    '''
    判断从OA里面获得的数据在rds明细表中是否存在
    :param app2:  sql语句执行对象
    :param fNumber: 编码
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''
    sql=f"select * from {tableName} where {FNumber}='{FNumber_value}'"

    if app2.select(sql)==[]:
        return True
    else :
        return False



def changeStatus(app2,status,tableName,param,param2):
    '''
    改变数据状态
    :param app2: sql语句执行对象
    :param status: 状态
    :param tableName: 表名
    :param param: 条件名
    :param param2: 条件
    :return:
    '''
    sql=f"update a set a.Fisdo={status} from {tableName} a where {param}='{param2}'"

    app2.update(sql)

def getStatus(app2,fNumber,tableName):
    '''
    获得数据状态
    :param app2: sql语句执行对象
    :param fNumber: 编码
    :param tableName: 表名
    :return:
    '''

    sql=f"select Fisdo from {tableName} where FNumber='{fNumber}'"

    result=app2.select(sql)

    if result != []:

        res = result[0]['Fisdo']

        if res == 1:
            return False
        elif res == 0:
            return True
        else:
            return 2

def getTaxRateCode(app2,param):
    '''
    转换税率编码
    :param app2: sql语句执行对象
    :param param: 条件
    :return:
    '''

    if param=="1":
        param=13
    elif param=="0":
        param="零"

    sql=f"select FNUMBER from rds_vw_taxrate where  FNAME like '{param}%'"
    res=app2.select(sql)

    if res==[]:
        return ""
    else:
        return res[0]['FNUMBER']

def getOrganizationCode(app2,FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg=="赛普总部":
        FUseOrg="苏州赛普"
    elif FUseOrg=="南通分厂":
        FUseOrg="赛普生物科技（南通）有限公司"

    sql=f"select FORGID from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    res=app2.select(sql)

    if res==[]:
        return ""
    else:
        return res[0]['FORGID']


def getOrganizationFNumber(app2,FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg=="赛普总部":
        FUseOrg="苏州赛普"
    elif FUseOrg=="南通分厂":
        FUseOrg="赛普生物科技（南通）有限公司"

    sql=f"select FNumber,FORGID from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    res=app2.select(sql)

    if res==[]:
        return ""
    else:
        return res[0]

def exchangeBooleanValue(param):
    '''
    逻辑值转换
    :param param:
    :return:
    '''

    if param=="是":
        return param
    elif param=="否":
        return param

def exchangeDateCode(param):
    if param=="天":
        return "1"
    elif param=="周":
        return "2"
    elif param=="月":
        return "3"

def getCountryCode(app2,param):

    sql=f"select FNUMBER from rds_vw_country where FNAME='{param}'"

    res=app2.select(sql)

    if res==[]:
        return ""
    else:
        return res[0]['FNUMBER']



def ListDateIsExist(app2,tableName,FName,FName_value,FStartDate,FStartDate_value):
    '''
    判断从OA里面获得的数据在rds列表中是否存在
    :param app2: sql语句执行对象
    :param js: 数据
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''
    sql=f"select * from {tableName} where {FName}='{FName_value}' and {FStartDate}='{FStartDate_value}'"
    if app2.select(sql)==[]:
        return True
    else:
        return False


def md5_encryption(now_time):

    m = hashlib.md5()
    username = "GYS"
    password = "abcgys123"
    token = username + password + now_time
    m.update(token.encode())
    md5 = m.hexdigest()

    return md5

def getOAListW(FVarDateTime):
    '''
    通过日期获取OA供应商列表数据
    :param FVarDateTime:
    :return:
    '''

    try:

        now = time.localtime()
        now_time = time.strftime("%Y%m%d%H%M%S", now)

        data = {
            "operationinfo": {
                "operator": "DMS"
            },
            'mainTable': {
                "FDate": FVarDateTime,
            },
            "pageInfo": {
                "pageNo": "1",
                "pageSize": "10000"
            },
            "header": {
                "systemid": "GYS",
                "currentDateTime": now_time,
                "Md5": md5_encryption(now_time)
            }
        }

        str = json.dumps(data, indent=2)

        values = parse.quote(str).replace("%20", "")

        url="http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/GYSList"

        payload = 'datajson=' + values
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        info = response.json()['result']

        lis = []
        t1 = time.time()
        t_today = time.strftime("%Y-%m-%d", time.localtime(t1))
        for i in json.loads(info):
            if i['mainTable']['FDate'] == t_today:
                lis.append(i)

        return lis

    except Exception as e:

        return []


def getOAListByFNumber(FNumber):
    '''
    通过单号获取OA供应商列表数据
    :param FVarDateTime:
    :return:
    '''
    try:

        now = time.localtime()
        now_time = time.strftime("%Y%m%d%H%M%S", now)

        data = {
            "operationinfo": {
                "operator": "DMS"
            },
            'mainTable': {
                "FNumber": FNumber,
            },
            "pageInfo": {
                "pageNo": "1",
                "pageSize": "10000"
            },
            "header": {
                "systemid": "GYS",
                "currentDateTime": now_time,
                "Md5": md5_encryption(now_time)
            }
        }

        str = json.dumps(data, indent=2)

        values = parse.quote(str).replace("%20", "")

        url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/GYSList"

        payload = 'datajson=' + values
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        info = response.json()['result']

        lis = []
        t1 = time.time()
        t_today = time.strftime("%Y-%m-%d", time.localtime(t1))
        for i in json.loads(info):
            if i['mainTable']['FDate'] == t_today:
                lis.append(i)

        return lis

    except Exception as e:

        return []

def getOAListN(FVarDateTime):
    now = time.localtime()
    now_time = time.strftime("%Y%m%d%H%M%S", now)

    data = {
        "operationinfo": {
            "operator": "DMS"
        },
        'mainTable': {
            "FDate": FVarDateTime,
        },
        "pageInfo": {
            "pageNo": "1",
            "pageSize": "10000"
        },
        "header": {
            "systemid": "GYS",
            "currentDateTime": now_time,
            "Md5": md5_encryption(now_time)
        }
    }

    str = json.dumps(data, indent=2)

    values = parse.quote(str).replace("%20", "")

    url="http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/GYSToday"

    payload = 'datajson=' + values
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    info = response.json()['result']

    js = json.loads(info)

    return js

def getOADetailDataN(FName,FDate):
    '''

    :param option:
    :param FNumber:
    :return:
    '''
    now = time.localtime()
    now_time = time.strftime("%Y%m%d%H%M%S", now)

    data = {
        "operationinfo": {
            "operator": "DMS"
        },
        'mainTable': {
            "FDate": FDate,
            "FName": FName
        },
        "pageInfo": {
            "pageNo": "1",
            "pageSize": "10000"
        },
        "header": {
            "systemid": "GYS",
            "currentDateTime": now_time,
            "Md5": md5_encryption(now_time)
        }
    }

    strs = json.dumps(data, indent=2)

    values = parse.quote(strs).replace("%20", "")

    url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/GYSReturn"

    payload = 'datajson=' + values
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    info = response.json()['result']

    js = json.loads(info)

    return js

def getOADetailDataW(FName,FDate):
    '''

    :param FDate:
    :param FName:
    :return:
    '''

    try:
        now = time.localtime()
        now_time = time.strftime("%Y%m%d%H%M%S", now)

        data = {
            "operationinfo": {
                "operator": "DMS"
            },
            'mainTable': {
                "FDate": FDate,
                "FName": FName
            },
            "pageInfo": {
                "pageNo": "1",
                "pageSize": "10000"
            },
            "header": {
                "systemid": "GYS",
                "currentDateTime": now_time,
                "Md5": md5_encryption(now_time)
            }
        }

        strs = json.dumps(data, indent=2)

        values = parse.quote(strs).replace("%20", "")

        url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/GYSReturn"

        payload = 'datajson=' + values
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        info = response.json()['result']

        js = json.loads(info)

        return js

    except Exception as e:

        return []

def supplierISExist(app2,FName,FOrgNumber):
    '''
    通过供应商的名字到系统查看供应商是否存在
    :param app2:
    :param FNumber:
    :param FOrgNumber:
    :return:
    '''

    sql=f"select * from rds_vw_supplier where FNAME='{FName}' and FORGNUMBER='{FOrgNumber}'"

    res=app2.select(sql)

    return res

def codeConversion(app2,FTableName,FName):
    '''
    编码值转换
    :param app2:
    :param FName:
    :return:
    '''

    sql=f"select FNUMBER from {FTableName} where FNAME='{FName}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FNUMBER']

def codeConversionOrg(app2,FTableName,FName,FOrg):
    '''
    编码值转换
    :param app2:
    :param FName:
    :return:
    '''

    sql=f"select FNUMBER from {FTableName} where FNAME='{FName}' and FOrgNumber='{FOrg}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FNUMBER']



def insert_into_listSource(data, app3):
    '''
    将数据插入source
    :param FVarDateTime:
    :return:
    '''

    for i in data:

        if i['mainTable']['FStatus'] == '已审核':

            if ListDateIsExist(app3, "RDS_OA_SRC_bd_SupplierList", "FSupplierName", i['mainTable']['FName'],
                                         "FStartDate", i['mainTable']['FVarDateTime']):
                sql = f"insert into RDS_OA_SRC_bd_SupplierList(FInterId,FStartDate,FEndDate,FApplyOrgName,FSupplierName,FUploadDate,Fisdo ) values({str(getFinterId(app3, 'RDS_OA_SRC_bd_SupplierList') + 1)},'{(i['mainTable'])['FVarDateTime']}',getdate(),'{(i['mainTable'])['FUseOrg']}','{(i['mainTable'])['FName']}',getdate(),0)"

                insertData(app3, sql)


def insert_into_ERP(erp_token, data, app2, app3):
    '''
    将数据插入到ERP系统
    :param erp_token:
    :param data:
    :param app2:
    :param app3:
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    res=ERP_suppliersave(api_sdk, erp_token, data, app2, app3)

    return res


def judgeDetailData(erp_token, app2, app3):
    '''
    判断RDS_OA_ODS_bd_SupplierDetail表中是否有数据
    :param app3:
    :return:
    '''

    sql = "select top 1 FInterId ,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCountry,FZipCode,FUniversalCode,FRegisterAddress,FMngrDeptName,FMngrMan,FSullierType,FInvoiceType,FTaxRate,FAccountNumber,FAccountName ,FBankTransferCode,FBankName,FBankAddr,FContact,FMobile,FEMail, FSupplierCategoryNo,FSupplierGradeNo ,FPriceListNo,FSettleCurrencyNo,FSettlementMethodNo,FPaymentConditionNo,FCurrencyNo,FUploadDate,Fisdo from RDS_OA_ODS_bd_SupplierDetail where Fisdo=0"

    res = app3.select(sql)

    if res != []:

        res=insert_into_ERP(erp_token=erp_token, data=res, app2=app2, app3=app3)

        return res

    else:

        return "没有需要同步的供应商"


def judgeDetailDataByFNumber(erp_token, app2, app3,FNumber):
    '''
    判断RDS_OA_ODS_bd_SupplierDetail表中是否有数据
    :param app3:
    :return:
    '''

    sql = f"select FInterId ,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCountry,FZipCode,FUniversalCode,FRegisterAddress,FMngrDeptName,FMngrMan,FSullierType,FInvoiceType,FTaxRate,FAccountNumber,FAccountName ,FBankTransferCode,FBankName,FBankAddr,FContact,FMobile,FEMail, FSupplierCategoryNo,FSupplierGradeNo ,FPriceListNo,FSettleCurrencyNo,FSettlementMethodNo,FPaymentConditionNo,FCurrencyNo,FUploadDate,Fisdo from RDS_OA_ODS_bd_SupplierDetail where FNumber='{FNumber}'"

    res = app3.select(sql)

    if res != []:

        insert_into_ERP(erp_token=erp_token, data=res, app2=app2, app3=app3)

    else:

        return "明细表中没有这个供应商"


def insert_into_detailSource(app3, data):
    '''
    将明细信息插入RDS_OA_SRC_bd_SupplierDetail表
    :param app3:
    :param data:
    :return:
    '''

    for i in data:

        d = getOADetailDataW(str(i['FSupplierName']), str(i['FEndDate']))

        if d != []:
            try:

                if DetailDateIsExist(app3, "FNumber", d[0]['mainTable']['FNumber'],
                                               "RDS_OA_SRC_bd_SupplierDetail"):
                    sql3 = f"insert into RDS_OA_SRC_bd_SupplierDetail(FInterId ,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCountry,FZipCode,FUniversalCode,FRegisterAddress,FMngrDeptName,FMngrMan,FSullierType,FInvoiceType,FTaxRate,FAccountNumber,FAccountName ,FBankTransferCode,FBankName,FBankAddr,FContact,FMobile,FEMail, FSupplierCategoryNo,FSupplierGradeNo ,FPriceListNo,FSettleCurrencyNo,FSettlementMethodNo,FPaymentConditionNo,FCurrencyNo,FUploadDate,Fisdo ) values({str(getFinterId(app3, 'RDS_OA_SRC_bd_SupplierDetail') + 1)},'{(d[0]['mainTable'])['FUseOrg']}','{(d[0]['mainTable'])['FDeptId1']}','{(d[0]['mainTable'])['FUserId']}','{(d[0]['mainTable'])['FVarDateTime']}','{(d[0]['mainTable'])['FNumber']}','{(d[0]['mainTable'])['FName']}','{(d[0]['mainTable'])['FShortName']}','{(d[0]['mainTable'])['FCountry']}','{(d[0]['mainTable'])['FZip']}','{(d[0]['mainTable'])['FSOCIALCRECODE']}','{(d[0]['mainTable'])['FRegisterAddress']}','{(d[0]['mainTable'])['FDeptId']}','{(d[0]['mainTable'])['FStaffId']}','{(d[0]['mainTable'])['FSupplyClassify']}','{(d[0]['mainTable'])['FInvoiceType']}','{(d[0]['mainTable'])['FTaxRateName']}','{(d[0]['mainTable'])['FBankCode']}','{(d[0]['mainTable'])['FBankHolder']}','{(d[0]['mainTable'])['FCNAPS']}','{(d[0]['mainTable'])['FOpenBankName']}','{(d[0]['mainTable'])['FOpenAddressRec']}','{(d[0]['mainTable'])['FContact']}','{(d[0]['mainTable'])['FMobile']}','{(d[0]['mainTable'])['FEMail']}','{(d[0]['mainTable'])['FSupplierClassifyNo']}','{(d[0]['mainTable'])['FSupplierGradeNo']}','{(d[0]['mainTable'])['FPRICELISTNO']}','{(d[0]['mainTable'])['FPayCurrencyNo']}','{(d[0]['mainTable'])['FSettlementNo']}','{(d[0]['mainTable'])['FPayConditionNo']}','{(d[0]['mainTable'])['FBankCurrencyNo']}',getdate(),0)"

                    insertData(app3, sql3)

                    changeStatus(app3, "1", 'RDS_OA_ODS_bd_SupplierList', "FSupplierName",
                                           (d[0]['mainTable'])['FName'])
                    print(f"该编码{d[0]['mainTable']['FNumber']}已保存到SRC中")

                else:
                    print(f"该编码{d[0]['mainTable']['FNumber']}已存在于数据库")
                    changeStatus(app3, "2", 'RDS_OA_SRC_bd_SupplierDetail', "FNumber", d[0]['mainTable']['FNumber'])
            except:
                changeStatus(app3, "2", 'RDS_OA_ODS_bd_SupplierList', "FSupplierName", i['FSupplierName'])
        else:
            print(f"该公司名称{i['FSupplierName']}不在今日审批中")
            changeStatus(app3, "2", 'RDS_OA_ODS_bd_SupplierList', "FSupplierName", i['FSupplierName'])


def judgeListData(app3):
    '''
    判断RDS_OA_ODS_bd_SupplierList表中是否有新增的数据
    :param app3:
    :return:
    '''

    sql = "select FSupplierName,FStartDate,FEndDate from RDS_OA_ODS_bd_SupplierList where Fisdo=0"

    res = app3.select(sql)

    if res != []:

        insert_into_detailSource(app3, res)

    else:

        pass



def judgeOAData(FNumber, app3):
    '''
    将数据插入source
    :param FVarDateTime:
    :return:
    '''

    OADataList = getOAListByFNumber(FNumber=FNumber)

    if OADataList != []:

        insert_into_listSource(OADataList, app3)

    else:

        pass


def ERP_suppliersave(api_sdk, option, dData, app2,app3):
    '''
    将数据进行保存
    :param option:
    :param dData:
    :return:
    '''

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in dData:

        # getStatus(app3,i['FNumber'],'RDS_OA_ODS_bd_SupplierDetail') and

        if supplierISExist(app2, i['FName'], "100") == []:

            changeStatus(app3, "0", "RDS_OA_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])

            if getStatus(app3, i['FNumber'], 'RDS_OA_ODS_bd_SupplierDetail'):
                model = {
                    "Model": {
                        "FSupplierId": 0,
                        "FCreateOrgId": {
                            "FNumber": "100"
                        },
                        "FUseOrgId": {
                            "FNumber": "100"
                        },
                        "FGroup": {
                            "FNumber": i['FSupplierCategoryNo']
                        },
                        "FName": i['FName']
                        # "FNumber": i['FNumber']
                        ,
                        "FShortName": i['FShortName'],
                        "FBaseInfo": {
                            "FCountry": {
                                "FNumber": "China" if i['FCountry'] == "" or i[
                                    'FCountry'] == "中国" else getCountryCode(
                                    app2, i['FCountry'])
                            },
                            "FSOCIALCRECODE": i['FUniversalCode'],
                            "FRegisterAddress": i['FRegisterAddress'],
                            "FZip": i['FZipCode'],
                            "FFoundDate": str(i['FDate']),
                            "FRegisterCode": str(i['FUniversalCode']),
                            "FSupplyClassify": "CG" if i['FSullierType'] == "" else getSullierTypeCode(
                                i['FSullierType']),
                            "FSupplierGrade": {
                                "FNumber": i['FSupplierGradeNo']
                            }
                        },
                        "FBusinessInfo": {
                            "FSettleTypeId": {
                                "FNumber": i['FSettlementMethodNo']
                            },
                            "FPRICELISTID": {
                                "FNumber": i['FPriceListNo']
                            },
                            "FVmiBusiness": False,
                            "FEnableSL": False
                        },
                        "FFinanceInfo": {
                            "FPayCurrencyId": {
                                "FNumber": "PRE001" if i['FCurrencyNo'] == "" else i['FCurrencyNo']
                            },
                            "FPayCondition": {
                                "FNumber": i['FPaymentConditionNo']
                            },
                            "FTaxType": {
                                "FNumber": "SFL02_SYS"
                            },
                            "FTaxRegisterCode": str(i['FUniversalCode']),
                            "FInvoiceType": "1" if (i['FInvoiceType'] == "" or i['FInvoiceType'] == "增值税专用发票") else "2",
                            "FTaxRateId": {
                                "FNUMBER": "SL02_SYS" if i['FTaxRate'] == "" else getTaxRateCode(app2, i['FTaxRate'])
                            }
                        },
                        "FBankInfo": [
                            {
                                "FBankCountry": {
                                    "FNumber": "China" if i['FCountry'] == "" or i[
                                        'FCountry'] == "中国" else getCountryCode(app2, i['FCountry'])
                                },
                                "FBankCode": i['FAccountNumber'],
                                "FBankHolder": i['FAccountName'],
                                "FOpenBankName": i['FBankName'],
                                "FCNAPS": i['FBankTransferCode'],
                                "FOpenAddressRec": i['FBankAddr'],
                                "FBankCurrencyId": {
                                    "FNumber": "PRE001" if i['FCurrencyNo'] == "" else i['FCurrencyNo']
                                },
                                "FBankIsDefault": False
                            }
                        ],
                        "FSupplierContact": [
                            {
                                "FContactId": 0,
                                "FContact ": i['FContact'],
                                "FMobile": i['FMobile'],
                                "FEMail": i['FEMail']
                            }
                        ]
                    }
                }
                res = api_sdk.Save("BD_Supplier", model)
                print("保存数据结果为:" + res)

                rj = json.loads(res)
                k3FNumber = rj['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']
                # print(res)
                #       rj是保存后的结果

                if rj['Result']['ResponseStatus']['IsSuccess']:

                    returnResult = ERP_suppliersubmit(rj['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'],
                                                      api_sdk)
                    #           rs是提交后的结果
                    rs = json.loads(returnResult)

                    if rs['Result']['ResponseStatus']['IsSuccess']:
                        resAudit = ERP_audit('BD_Supplier',
                                             rs['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'],
                                             api_sdk)
                        ra = json.loads(resAudit)
                        # ra是审核后的结果信息
                        if ra['Result']['ResponseStatus']['IsSuccess']:
                            r = ERP_allocate('BD_Supplier', getCodeByView('BD_Supplier', rs['Result']['ResponseStatus'][
                                'SuccessEntitys'][0]['Number'], api_sdk),
                                             getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                            AlloctOperation(api_sdk, i, app2, k3FNumber)

                            changeStatus(app3, "1", "RDS_OA_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                            changeStatus(app3, "1", "RDS_OA_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])

                            inser_logging(app3, '供应商', i['FNumber'], i['FNumber'] + "保存成功"
                                          , FIsdo=1)
                            return "同步成功"

                        else:
                            changeStatus(app3, "2", "RDS_OA_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                            changeStatus(app3, "2", "RDS_OA_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])

                            inser_logging(app3, '供应商', i['FNumber'], ra
                                          , FIsdo=2)
                    else:
                        changeStatus(app3, "2", "RDS_OA_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "2", "RDS_OA_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                        inser_logging(app3, '供应商', i['FNumber'], rs
                                      , FIsdo=2)
                else:
                    changeStatus(app3, "2", "RDS_OA_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                    changeStatus(app3, "2", "RDS_OA_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                    inser_logging(app3, '供应商', i['FNumber'], rj
                                  , FIsdo=2)
        else:

            return "该编码{}已存在于金蝶".format(i['FNumber'])




def inser_logging(app, FProgramName, FNumber, FMessage, FIsdo,
                  FOccurrenceTime=str(datetime.datetime.now())[:19],
                  FCompanyName='CP'):
    sql = "insert into RDS_OA_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName,FIsdo) values('" + FProgramName + "','" + FNumber + "','" + FMessage + "','" + FOccurrenceTime + "','" + FCompanyName + "'," + str(
        FIsdo) + ")"
    data = app.insert(sql)
    return data


def ERP_suppliersubmit(number, api_sdk):
    '''
    对创建的数据进行提交
    :param number 单据编号:
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Numbers": [number],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": ""
    }

    res = api_sdk.Submit("BD_Supplier", data)

    return res


def ERP_audit(forbid, number, api_sdk):
    '''
    将状态为审核中的数据审核
    :param forbid: 表单ID
    :param number: 编码
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Numbers": [number],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": ""
    }

    res = api_sdk.Audit(forbid, data)

    return res


def ERP_allocate(forbid, PkIds, TOrgIds, api_sdk):
    '''
    分配
    :param forbid: 表单
    :param PkIds: 被分配的基础资料内码集合
    :param TOrgIds: 目标组织内码集合
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "PkIds": int(PkIds),
        "TOrgIds": TOrgIds
    }

    res = api_sdk.Allocate(forbid, data)

    return res


def getCodeByView(forbid, number, api_sdk):
    '''
    通过编码找到对应的内码
    :param forbid: 表单ID
    :param number: 编码
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Number": number,
        "Id": "",
        "IsSortBySeq": "false"
    }
    # 将结果转换成json类型
    rs = json.loads(api_sdk.View(forbid, data))
    res = rs['Result']['Result']['Id']

    return res


def AlloctOperation(api_sdk, i,app2, k3FNumber):
    '''
    数据分配后进行提交审核
    :param forbid:
    :param number:
    :param api_sdk:
    :return:
    '''

    SaveAfterAllocation(api_sdk, i,app2, k3FNumber)


def SaveAfterAllocation(api_sdk, i, app2, k3FNumber):
    FOrgNumber = getOrganizationFNumber(app2, i['FApplyOrgName'])

    model = {
        "Model": {
            "FSupplierId": queryDocuments(app2, api_sdk, k3FNumber, FOrgNumber['FORGID']),
            "FCreateOrgId": {
                "FNumber": "100"
            },
            "FUseOrgId": {
                "FNumber": str(FOrgNumber['FNumber'])
            },
            "FGroup": {
                "FNumber": i['FSupplierCategoryNo']
            },
            "FName": str(i['FName']),
            'FNumber': str(k3FNumber),
            "FShortName": i['FShortName'],
            "FBaseInfo": {
                "FCountry": {
                    "FNumber": "China" if i['FCountry'] == "" or i['FCountry'] == "中国" else getCountryCode(app2, i[
                        'FCountry'])
                },
                "FSOCIALCRECODE": i['FUniversalCode'],
                "FRegisterAddress": i['FRegisterAddress'],
                "FDeptId": {
                    "FNumber": 'BM000040' if str(FOrgNumber['FNumber']) == '104' else codeConversionOrg(app2,
                                                                                                           "rds_vw_department",
                                                                                                           i[
                                                                                                               'FMngrDeptName'],
                                                                                                           str(
                                                                                                               FOrgNumber[
                                                                                                                   'FNumber']))
                },
                "FStaffId": {
                    "FNumber": codeConversion(app2, "rds_vw_employees", i['FMngrMan'])
                },
                "FZip": i['FZipCode'],
                "FFoundDate": str(i['FDate']),
                "FRegisterCode": str(i['FUniversalCode']),
                "FSupplyClassify": "CG" if i['FSullierType'] == "" else getSullierTypeCode(i['FSullierType']),
                "FSupplierGrade": {
                    "FNumber": i['FSupplierGradeNo']
                }
            },
            "FBusinessInfo": {
                "FSettleTypeId": {
                    "FNumber": str(i['FSettlementMethodNo'])
                },
                "FPRICELISTID": {
                    "FNumber": str(i['FPriceListNo'])
                },
                "FProviderId": {
                    "FNumber": str(k3FNumber)
                },
                "FVmiBusiness": False,
                "FEnableSL": False
            },
            "FFinanceInfo": {
                "FPayCurrencyId": {
                    "FNumber": "PRE001" if i['FCurrencyNo'] == "" else i['FCurrencyNo']
                },
                "FPayCondition": {
                    "FNumber": i['FPaymentConditionNo']
                },
                "FSettleId": {
                    "FNumber": str(k3FNumber)
                },
                "FTaxType": {
                    "FNumber": "SFL02_SYS"
                },
                "FTaxRegisterCode": str(i['FUniversalCode']),
                "FChargeId": {
                    "FNumber": str(i['FNumber'])
                },
                "FInvoiceType": "1" if (i['FInvoiceType'] == "" or i['FInvoiceType'] == "增值税专用发票") else "2",
                "FTaxRateId": {
                    "FNUMBER": "SL02_SYS" if i['FTaxRate'] == "" else getTaxRateCode(app2, i['FTaxRate'])
                }
            },
        }
    }
    res = json.loads(api_sdk.Save("BD_Supplier", model))
    print("修改数据结果为:" + str(res))

    if res['Result']['ResponseStatus']['IsSuccess']:
        submit_res = json.loads(ERP_suppliersubmit(str(k3FNumber), api_sdk))
        audit_res = json.loads(ERP_audit("BD_Supplier", str(k3FNumber), api_sdk))
        print("修改数据提交结果为:" + str(submit_res))
        print("修改数据审核结果为:" + str(audit_res))


def queryDocuments(app2, api_sdk, number, forgid):
    sql = f"""
        select a.FNUMBER,a.FSUPPLIERID,a.FMASTERID,a.FUSEORGID,a.FCREATEORGID,b.FNAME from T_BD_SUPPLIER  
        a inner join takewiki_t_organization b
        on a.FUSEORGID = b.FORGID
        where a.FNUMBER = '{number}' and b.FORGID = '{forgid}'
        """
    res = app2.select(sql)

    if res != []:

        return res[0]['FSUPPLIERID']

    else:

        return "0"


def judgeDate(FNumber, api_sdk):
    '''
    查看数据是否在ERP系统存在
    :param FNumber: 物料编码
    :param api_sdk:
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res = json.loads(api_sdk.View("BD_Supplier", data))

    return res['Result']['ResponseStatus']['IsSuccess']


def supplierInterfaceByDate(option1, FVarDateTime, erp_token, china_token):
    '''
    功能入口函数
    :param option1: ERP用户信息
    :param FVarDateTime: 日期
    :param token:  操作数据库底层包token
    :return:
    '''
    app2 = RdClient(token=erp_token)
    app3 = RdClient(token=china_token)

    judgeOAData(FVarDateTime, app3)

    judgeListData(app3)

    res=judgeDetailData(option1, app2, app3)

    return res



def supplierInterfaceByFNumber(option1, FNumber, erp_token, china_token):
    '''
    功能入口函数 按编号
    :param option1: ERP用户信息
    :param FVarDateTime: 日期
    :param token:  操作数据库底层包token
    :return:
    '''
    app2 = RdClient(token=erp_token)
    app3 = RdClient(token=china_token)

    judgeOAData(FNumber=FNumber, app3=app3)

    judgeListData(app3=app3)

    res=judgeDetailDataByFNumber(erp_token=option1, app2=app2, app3=app3,FNumber=FNumber)

    return res