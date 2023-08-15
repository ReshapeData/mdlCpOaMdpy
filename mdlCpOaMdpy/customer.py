#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import requests
import hashlib
import time
from urllib import parse
from pyrda.dbms.rds import RdClient
from time import sleep
from k3cloud_webapi_sdk.main import K3CloudApiSdk


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

def ERP_customersave(api_sdk, option, dData, app2,app3):
    '''
    将数据进行保存
    :param option:
    :param dData:
    :return:
    '''

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])
    ret_data = []
    for i in dData:

        if  checkCustomerExist(app2,i['FName']) == []:
            model = {
                "Model": {
                    "FCUSTID": 0,
                    "FCreateOrgId": {
                        "FNumber": "100"
                    },
                    "FUseOrgId": {
                        "FNumber": "100"
                    },
                    "FName": i['FName'],
                    "FShortName": i['FShortName'],
                    "FCOUNTRY": {
                        "FNumber": get_num(app2, '国家', i['FCOUNTRY'])
                    },
                    "FTEL": i['FTEL'],
                    "FINVOICETITLE": i['FINVOICETITLE'],
                    "FTAXREGISTERCODE": i['FTAXREGISTERCODE'],
                    "FINVOICEBANKNAME": i['FBankName'],
                    "FINVOICETEL": i['FINVOICETEL'],
                    "FINVOICEBANKACCOUNT": i['FAccountNumber'],
                    "FINVOICEADDRESS": i['FINVOICEADDRESS'],
                    "FSOCIALCRECODE": i['FTAXREGISTERCODE'],
                    "FIsGroup": False,
                    "FIsDefPayer": False,
                    "F_SZSP_Text": i['F_SZSP_Text'],
                    'FSETTLETYPEID': {
                        "FNumber": i['FSETTLETYPENO'],
                    },
                    "FRECCONDITIONID": {
                        "FNumber": i['FRECCONDITIONNO'],
                    },
                    "F_SZSP_KHZYJB": {
                        "FNumber": i['F_SZSP_KHZYJBNo']  # 客户重要级别
                    },
                    "F_SZSP_KHGHSX": {
                        "FNumber": i['F_SZSP_KHGHSXNo']  # 客户公海属性
                    },
                    "F_SZSP_XSMS": {
                        "FNumber": i['F_SZSP_XSMSNo']  # 销售模式
                    },
                    "F_SZSP_XSMSSX": {
                        "FNumber": i['F_SZSP_XSMSSXNo']  # 销售模式属性
                    },
                    'F_SZSP_BLOCNAME': i['F_SZSP_BLOCNAME'],
                    "FCustTypeId": {
                        "FNumber": get_num(app2, '客户类别', i['FCustTypeNo'])  # 客户价格属性
                    },
                    "FGroup": {
                        "FNumber": i['FGroupNo']  # 客户分组
                    },
                    "FTRADINGCURRID": {
                        "FNumber": i['FTRADINGCURRNO']
                    },
                    "FInvoiceType": "1" if i['FINVOICETYPE'] == "" or i['FINVOICETYPE'] == "增值税专用发票" else "2",
                    "FTaxType": {
                        "FNumber": "SFL02_SYS"
                    },
                    "FTaxRate": {
                        "FNumber": "SL02_SYS" if i['FTaxRate'] == "" else getcode(app2, "FNUMBER", "rds_vw_taxRate",
                                                                                     "FNAME", i['FTaxRate'])
                    },
                    "FISCREDITCHECK": True,
                    "FIsTrade": True,
                    "FUncheckExpectQty": False,
                    "F_SZSP_KHFL": {
                        "FNumber": i['F_SZSP_KHFLNo']
                    },
                    "FT_BD_CUSTOMEREXT": {
                        "FEnableSL": False,
                        "FALLOWJOINZHJ": False
                    },
                    "FT_BD_CUSTBANK": [
                        {
                            "FENTRYID": 0,
                            "FCOUNTRY1": {
                                "FNumber": get_num(app2, '国家', i['FCOUNTRY'])
                            },
                            "FBANKCODE": i['FAccountNumber'],
                            "FACCOUNTNAME": i['FINVOICETITLE'],
                            "FBankTypeRec": {
                                "FNUMBER": ""
                            },
                            "FTextBankDetail": "",
                            "FBankDetail": {
                                "FNUMBER": ""
                            },
                            "FOpenAddressRec": "",
                            "FOPENBANKNAME": i['FBankName'],
                            "FCNAPS": "",
                            "FCURRENCYID": {
                                "FNumber": get_num(app2, '国家', i['FCOUNTRY'])
                            },
                            "FISDEFAULT1": "false"
                        }
                    ],
                }
            }

            savedResultInformation = api_sdk.Save("BD_Customer", model)
            print(f"编码为：{savedResultInformation}")
            sri = json.loads(savedResultInformation)

            if sri['Result']['ResponseStatus']['IsSuccess']:
                submittedResultInformation = ERP_customersubmit(
                    sri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'], api_sdk)
                print(f"编码为：{submittedResultInformation}数据提交成功")

                subri = json.loads(submittedResultInformation)
                ret_data.append(sri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'] + '保存成功')
                if subri['Result']['ResponseStatus']['IsSuccess']:

                    k3FNumber = subri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                    auditResultInformation = ERP_audit('BD_Customer',
                                                       k3FNumber,
                                                       api_sdk)

                    auditres = json.loads(auditResultInformation)

                    if auditres['Result']['ResponseStatus']['IsSuccess']:

                        result = ERP_allocate('BD_Customer', getCodeByView('BD_Customer',
                                                                           k3FNumber, api_sdk),
                                              getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                        AlloctOperation('BD_Customer',
                                        k3FNumber, api_sdk, i,
                                         app2)

                        changeStatus(app3, "1", "RDS_OA_SRC_bd_CustomerDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "1", "RDS_OA_ODS_bd_CustomerDetail", "FNumber", i['FNumber'])
                        inser_logging(app3, '客户', i['FNumber'], i['FNumber'] + "保存成功"
                                      , FIsdo=1)
                        update_approval(i['FNumber'], app2)


                    else:
                        changeStatus(app3, "2", "RDS_OA_SRC_bd_CustomerDetail", "FNumber", i['FNumber'])

                else:
                    changeStatus(app3, "2", "RDS_OA_SRC_bd_CustomerDetail", "FNumber", i['FNumber'])

            else:
                inser_logging(app3, '客户', i['FNumber'],
                              sri['Result']['ResponseStatus']['Errors'][0]['Message'], FIsdo=2)
                changeStatus(app3, "2", "RDS_OA_SRC_bd_CustomerDetail", "FNumber", i['FNumber'])

                ret_data.append(sri)
        else:
            inser_logging(app3, '客户', i['FNumber'], "该客户{}数据已存在于金蝶".format(i['FName']), FIsdo=2)
            return "{}已存在于金蝶".format(i['FName'])

    ret_dict = {
        "code": "1",
        "message": ret_data,

    }
    return ret_dict


def SaveAfterAllocation(api_sdk, i,app2, FNumber):
    FOrgNumber = getOrganizationFNumber(app2, i['FApplyOrgName'])

    model = {
        "Model": {
            "FCUSTID": queryDocuments(app2, FNumber, FOrgNumber['FORGID']),
            "FCreateOrgId": {
                "FNumber": "100"
            },
            "FUseOrgId": {
                "FNumber": str(FOrgNumber['FNUMBER'])
            },
            "FName": str(i['FName']),
            'FNumber': FNumber,
            "FCOUNTRY": {
                "FNumber": get_num(app2, '国家', i['FCOUNTRY'])
            },
            "FTRADINGCURRID": {
                "FNumber": i['FTRADINGCURRNO']
            },
            "FSALDEPTID": {
                "FNumber": getcode(app2, "FNUMBER", "rds_vw_department", "FNAME", i['FAalesDeptName'])
            },
            "FSALGROUPID": {
                "FNumber": i['FSalesGroupNo']
            },
            "FSELLER": {
                "FNumber": getcode(app2, "FNUMBER", "rds_vw_salesman", "FNAME", i['FSalesman'])
            },

        }
    }
    res = api_sdk.Save("BD_Customer", model)
    save_res = json.loads(res)
    if save_res['Result']['ResponseStatus']['IsSuccess']:
        submit_res = ERP_customersubmit(FNumber, api_sdk)
        audit_res = ERP_audit("BD_Customer", FNumber, api_sdk)

    print(f"修改编码为{FNumber}的信息:" + res)


def ERP_customersubmit(fNumber, api_sdk):
    '''
    提交
    :param fNumber:
    :param api_sdk:
    :return:
    '''
    model = {
        "CreateOrgId": 0,
        "Numbers": [fNumber],
        "Ids": "",
        "SelectedPostId": 0,
        "NetworkCtrl": "",
        "IgnoreInterationFlag": ""
    }
    res = api_sdk.Submit("BD_Customer", model)

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


def AlloctOperation(forbid, number, api_sdk, i,app2):
    '''
    数据分配后进行提交审核
    :param forbid:
    :param number:
    :param api_sdk:
    :return:
    '''

    SaveAfterAllocation(api_sdk, i,app2, number)


# def judgeDate(FNumber, api_sdk):
#     '''
#     查看数据是否在ERP系统存在
#     :param FNumber: 客户编码
#     :param api_sdk:
#     :return:
#     '''
#
#     data = {
#         "CreateOrgId": 0,
#         "Number": FNumber,
#         "Id": "",
#         "IsSortBySeq": "false"
#     }
#
#     res = json.loads(api_sdk.View("BD_Customer", data))
#
#     return res['Result']['ResponseStatus']['IsSuccess']


def queryDocuments(app2, number, name):
    sql = f"""
        select a.FNUMBER,a.FCUSTID,a.FMASTERID,a.FUSEORGID,a.FCREATEORGID,b.FNAME from T_BD_Customer
        a inner join takewiki_t_organization b
        on a.FUSEORGID = b.FORGID
        where a.FNUMBER = '{number}' and a.FUSEORGID = '{name}'
        """
    res = app2.select(sql)

    if res != []:

        return res[0]['FCUSTID']

    else:

        return "0"


def ExistFname(app2, table, name):
    sql = f"""
            select FNAME from {table} where FNAME = {name}
            """
    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def ERP_unAudit(api_sdk, FNumber):
    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "InterationFlags": "",
        "IgnoreInterationFlag": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": ""
    }
    res = json.loads(api_sdk.UnAudit("BD_Customer", model))

    if res['Result']['ResponseStatus']['IsSuccess']:
        return f'{FNumber}客户反审核成功'
    else:
        return res['Result']['ResponseStatus']['Errors'][0]['Message']


def ERP_delete(api_sdk, FNumber):
    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "NetworkCtrl": ""
    }
    res = json.loads(api_sdk.Delete("BD_Customer", model))
    if res['Result']['ResponseStatus']['IsSuccess']:
        return f'{FNumber}客户删除成功'
    else:
        return res['Result']['ResponseStatus']['Errors'][0]['Message']


def ERP_CancelAllocate(app2, rc, api_sdk, FNumber, FApplyOrgName):
    FOrgNumber = rc.getOrganizationFNumber(app2, FApplyOrgName)
    FCUSTID = queryDocuments(app2, FNumber, FOrgNumber['FORGID']) - 1
    model = {
        "PkIds": FCUSTID,
        "TOrgIds": str(FOrgNumber['FORGID'])
    }
    res = json.loads(api_sdk.CancelAllocate("BD_Customer", model))

    if res['Result']['ResponseStatus']['IsSuccess']:
        return f'{FNumber}客户取消分配成功'
    else:
        return res['Result']['ResponseStatus']['Errors'][0]['Message']


def inser_logging(app, FProgramName, FNumber, FMessage, FIsdo, FOccurrenceTime=str(datetime.datetime.now())[:19],
                  FCompanyName='CP'):
    sql = "insert into RDS_OA_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName,FIsdo) values('" + FProgramName + "','" + FNumber + "','" + FMessage + "','" + FOccurrenceTime + "','" + FCompanyName + "'," + str(
        FIsdo) + ")"
    data = app.insert(sql)
    return data


def get_num(app, typename, name):
    sql = "select FNUMBER from rds_vw_auxiliary where FNAME='{}' and FDATAVALUE = '{}'".format(typename, name)
    res = app.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ""


def get_cusgroup(app, name):
    sql = "select FNUMBER from rds_vw_customergroup where FNAME='{}'".format(name)
    res = app.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ""


def get_currency(app, name):
    sql = "select FNUMBER from rds_vw_currency where FNAME='{}'".format(name)
    res = app.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ""


def update_approval(num, app):
    """
    更新审批流状态
    :param num: 编码
    :return: NONE
    """
    sql1 = """UPDATE a
            SET a.FSTATUS = 1
            FROM
                T_WF_ASSIGN a
                LEFT JOIN T_WF_PROCINST b ON a.FPROCINSTID= b.FPROCINSTID 
            WHERE
                b.FNUMBER like '%{}%'""".format(num)

    sql2 = """
        UPDATE b 
            SET b.FSTATUS = 1 
        FROM
            T_WF_ASSIGN a
            LEFT JOIN T_WF_PROCINST b ON a.FPROCINSTID= b.FPROCINSTID 
        WHERE
            b.FNUMBER like '%{}%'""".format(num)
    app.update(sql1)
    app.update(sql2)


def getcode(app2, param, tableName, param1, param2):
    '''
    通过传入的参数获得相应的编码
    :param app2 执行sql语句对象:
    :param param 需要查询的字段:
    :param tableName 表名:
    :param param1 查询条件字段:
    :param param2 条件:
    :return 将查询的到的结果返回:
    '''

    if param2 == "销售四部":
        param2 = "国际销售部"

    sql = f"select {param} from {tableName} where {param1}='{param2}'"

    res = app2.select(sql)

    return res[0][f'{param}']


def getSullierTypeCode(param):
    '''
    转换code码
    :param param: 条件
    :return:
    '''
    d = {"采购": "CG", "委外": "WW", "服务": "FW", "综合": "ZH"}

    res = d[param]

    return res


def getFinterId(app2, tableName):
    '''
    在两张表中找到最后一列数据的索引值
    :param app2: sql语句执行对象
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''

    sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"

    res = app2.select(sql)

    return res[0]['FMaxId']


def DetailDateIsExist(app2, FNumber, FNumber_value, tableName):
    '''
    判断从OA里面获得的数据在rds明细表中是否存在
    :param app2:  sql语句执行对象
    :param fNumber: 编码
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''
    sql = f"select * from {tableName} where {FNumber}='{FNumber_value}'"

    if app2.select(sql) == []:
        return True
    else:
        return False


def ListDateIsExist(app2, tableName, FName, FName_value, FStartDate, FStartDate_value):
    '''
    判断从OA里面获得的数据在rds列表中是否存在
    :param app2: sql语句执行对象
    :param js: 数据
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''
    sql = f"select * from {tableName} where {FName}='{FName_value}' and {FStartDate}='{FStartDate_value}'"
    if app2.select(sql) == []:
        return True
    else:
        return False


def changeStatus(app2, status, tableName, param, param2):
    '''
    改变数据状态
    :param app2: sql语句执行对象
    :param status: 状态
    :param tableName: 表名
    :param param: 条件名
    :param param2: 条件
    :return:
    '''
    sql = f"update a set a.Fisdo={status} from {tableName} a where {param}='{param2}'"

    app2.update(sql)


def getStatus(app2, fNumber, tableName):
    '''
    获得数据状态
    :param app2: sql语句执行对象
    :param fNumber: 编码
    :param tableName: 表名
    :return:
    '''

    sql = f"select Fisdo from {tableName} where FNumber='{fNumber}'"

    if app2.select(sql) != []:

        res = app2.select(sql)[0]['Fisdo']

        if res == 1:
            return False
        elif res == 0:
            return True
        else:
            return 2


def getTaxRateCode(app2, param):
    '''
    转换税率编码
    :param app2: sql语句执行对象
    :param param: 条件
    :return:
    '''

    if param == "1":
        param = 13
    elif param == "0":
        param = "零"

    sql = f"select FNUMBER from rds_vw_taxrate where  FNAME like '{param}%'"
    res = app2.select(sql)

    return res


def getOrganizationCode(app2, FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg == "赛普总部":
        FUseOrg = "苏州赛普"
    elif FUseOrg == "南通分厂":
        FUseOrg = "赛普生物科技（南通）有限公司"

    sql = f"select FORGID from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    oResult = app2.select(sql)

    return oResult[0]['FORGID']


def exchangeBooleanValue(param):
    '''
    逻辑值转换
    :param param:
    :return:
    '''

    if param == "是":
        return param
    elif param == "否":
        return param


def exchangeDateCode(param):
    if param == "天":
        return "1"
    elif param == "周":
        return "2"
    elif param == "月":
        return "3"


def dateConstraint(date):
    list = date.split("-")

    year = int(list[0])

    month = int(list[1])

    day = int(list[2])

    return datetime.date(year, month, day)


def md5_encryption(now_time):
    m = hashlib.md5()
    username = "Customer"
    password = "abccus123"
    token = username + password + now_time
    m.update(token.encode())
    md5 = m.hexdigest()

    return md5


def getOAListW(FVarDateTime):

    try:

        now = time.localtime()
        now_time = time.strftime("%Y%m%d%H%M%S", now)

        data = {
            "operationinfo": {
                "operator": "DMS"
            },
            'mainTable': {
                "FDate": FVarDateTime,
                "FStatus": '1',
            },
            "pageInfo": {
                "pageNo": "1",
                "pageSize": "10000"
            },
            "header": {
                "systemid": "Customer",
                "currentDateTime": now_time,
                "Md5": md5_encryption(now_time)
            }
        }

        str = json.dumps(data, indent=2)

        values = parse.quote(str).replace("%20", "")

        url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/CustomerToday"

        payload = 'datajson=' + values
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        info = response.json()['result']
        # lis = []
        # t1 = time.time()
        # t_today = time.strftime("%Y-%m-%d", time.localtime(t1))
        # for i in json.loads(info):
        #     if i['mainTable']['FDate'] == t_today:
        #         lis.append(i)
        #
        # # print(lis)

        return json.loads(info)

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
            "systemid": "Customer",
            "currentDateTime": now_time,
            "Md5": md5_encryption(now_time)
        }
    }

    str = json.dumps(data, indent=2)

    values = parse.quote(str).replace("%20", "")

    url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/CustomerToday"

    payload = 'datajson=' + values
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    info = response.json()['result']

    js = json.loads(info)

    return js


def getOADetailDataW(FName, FName_value, FStartDate, FStartDate_value):
    '''

    :param option:
    :param FName:
    :param FName_value:
    :param FStartDate:
    :param FStartDate_value:
    :return:
    '''
    now = time.localtime()
    now_time = time.strftime("%Y%m%d%H%M%S", now)

    data = {
        "operationinfo": {
            "operator": "DMS"
        },
        'mainTable': {
            'FName2052': str(FName_value),
            FStartDate: str(FStartDate_value),
            "FStatus": '1'
        },
        "pageInfo": {
            "pageNo": "1",
            "pageSize": "10000"
        },
        "header": {
            "systemid": "Customer",
            "currentDateTime": now_time,
            "Md5": md5_encryption(now_time)
        }
    }

    strs = json.dumps(data, indent=2)

    values = parse.quote(strs).replace("%20", "")

    url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/CustomerReturn"

    payload = 'datajson=' + values
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    info = response.json()['result']

    js = json.loads(info)

    return js


def getOADetailDataN(FName, FName_value, FStartDate, FStartDate_value):
    '''

    :param option:
    :param FName:
    :param FName_value:
    :param FStartDate:
    :param FStartDate_value:
    :return:
    '''
    now = time.localtime()
    now_time = time.strftime("%Y%m%d%H%M%S", now)

    data = {
        "operationinfo": {
            "operator": "DMS"
        },
        'mainTable': {
            FName: str(FName_value),
            FStartDate: str(FStartDate_value)
        },
        "pageInfo": {
            "pageNo": "1",
            "pageSize": "10000"
        },
        "header": {
            "systemid": "Customer",
            "currentDateTime": now_time,
            "Md5": md5_encryption(now_time)
        }
    }

    strs = json.dumps(data, indent=2)

    values = parse.quote(strs).replace("%20", "")

    url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/CustomerReturn"

    payload = 'datajson=' + values
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    info = response.json()['result']

    js = json.loads(info)

    return js


def getOrganizationFNumber(app2, FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg == "赛普总部":
        FUseOrg = "苏州赛普"
    elif FUseOrg == "南通分厂":
        FUseOrg = "赛普生物科技（南通）有限公司"

    sql = f"select FORGID,FNUMBER  from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]


def checkCustomerExist(app2, FName):
    '''
    通过客户名称判断客户是否已存在
    :param app2:
    :param FName:
    :return:
    '''

    sql = f"select FNUMBER from rds_vw_customer where FNAME='{FName}'".encode('utf-8')

    res = app2.select(sql)

    return res


def getOAListByFNumber(FNumber):

    try:

        now = time.localtime()
        now_time = time.strftime("%Y%m%d%H%M%S", now)

        data = {
            "operationinfo": {
                "operator": "DMS"
            },
            'mainTable': {
                "FNumber": FNumber,
                "FStatus": '1',
            },
            "pageInfo": {
                "pageNo": "1",
                "pageSize": "10000"
            },
            "header": {
                "systemid": "Customer",
                "currentDateTime": now_time,
                "Md5": md5_encryption(now_time)
            }
        }

        str = json.dumps(data, indent=2)

        values = parse.quote(str).replace("%20", "")

        url = "http://192.168.1.15:32212/api/cube/restful/interface/getModeDataPageList/CustomerToday"

        payload = 'datajson=' + values
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        info = response.json()['result']

        return json.loads(info)

    except Exception as e:

        return []

def customerInterfaceByDate(option1, FVarDateTime, token_erp, token_china):
    '''
    通过日期同步客户
    :param option1:
    :param FVarDateTime:
    :param token_erp:
    :param token_china:
    :return:
    '''
    app3 = RdClient(token=token_erp)
    app2 = RdClient(token=token_china)
    OADataList = getOAListW(FVarDateTime)

    if OADataList == []:
        pass
    else:
        for i in OADataList:
            if ListDateIsExist(app2, "RDS_OA_SRC_bd_CustomerList", "FCustomerName", i['mainTable']['FName2052'],
                                  "FStartDate", i['mainTable']['FVarDateTime']) and i['mainTable']['FStatus'] == '已审核':
                sql1 = f"insert into RDS_OA_SRC_bd_CustomerList(FInterId,FStartDate,FEndDate,FApplyOrgName,FcustomerName,FUploadDate,FIsdo) values({getFinterId(app2, 'RDS_OA_SRC_bd_CustomerList') + 1},'{i['mainTable']['FVarDateTime']}',getdate(),'{i['mainTable']['FUseOrgIdName']}','{i['mainTable']['FName2052']}',getdate(),0)"
                insertData(app2, sql1)

        sql2 = "select FCustomerName,FStartDate,FEndDate from RDS_OA_ODS_bd_CustomerList where FIsdo = 0"
        res = getData(app2, sql2)

        for k in res:
            d = getOADetailDataW('FName2052', k['FCustomerName'], 'FVarDateTime', k['FEndDate'])
            if d != []:

                if DetailDateIsExist(app2, "FNumber", d[0]['mainTable']['FNameId'], "RDS_OA_ODS_bd_CustomerDetail"):
                    sql3 = f"insert into RDS_OA_SRC_bd_CustomerDetail(FInterId,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCOUNTRY,FPROVINCIAL,FTEL,FINVOICETITLE,FTAXREGISTERCODE,FBankName,FINVOICETEL,FAccountNumber,FINVOICEADDRESS,FINVOICETYPE,FTaxRate,FCONTACT,FBizAddress,FMOBILE,FSalesman,FAalesDeptName,FCustTypeNo,FGroupNo,F_SZSP_KHFLNo,FSalesGroupNo,FTRADINGCURRNO,FSETTLETYPENO,FRECCONDITIONNO,FPRICELISTNO,FUploadDate,FIsdo,F_SZSP_Text,F_SZSP_KHZYJBNo,F_SZSP_KHGHSXNo,F_SZSP_XSMSNo,F_SZSP_XSMSSXNo) values({getFinterId(app2, 'RDS_OA_SRC_bd_CustomerDetail') + 1},'{d[0]['mainTable']['FUseOrgIdName']}','{d[0]['mainTable']['FDeptId']}','{d[0]['mainTable']['FUserId']}','{d[0]['mainTable']['FVarDateTime']}','{d[0]['mainTable']['FNameId']}','{d[0]['mainTable']['FName2052']}','{d[0]['mainTable']['FShortName2052']}','{d[0]['mainTable']['FCOUNTRYName']}','{d[0]['mainTable']['FPROVINCIALName']}','{d[0]['mainTable']['FTEL']}','{d[0]['mainTable']['FINVOICETITLE']}','{d[0]['mainTable']['FTAXREGISTERCODE']}','{d[0]['mainTable']['FINVOICEBANKNAME']}','{d[0]['mainTable']['FINVOICETEL']}','{d[0]['mainTable']['FINVOICEBANKACCOUNT']}','{d[0]['mainTable']['FINVOICEADDRESS']}','{d[0]['mainTable']['FInvoiceType']}','{d[0]['mainTable']['FTaxRateName']}','{d[0]['mainTable']['FContactIdName']}','{d[0]['mainTable']['FADDRESS1']}','{d[0]['mainTable']['FMOBILE']}','{d[0]['mainTable']['FSELLERName1']}','{d[0]['mainTable']['FSALDEPTIDDeptId']}','{d[0]['mainTable']['FCustTypeNo']}','{d[0]['mainTable']['FGroupNo']}','{d[0]['mainTable']['F_SZSP_KHFLNo']}','{d[0]['mainTable']['FSalesGroupNo']}','{d[0]['mainTable']['FTRADINGCURRNO']}','{d[0]['mainTable']['FSETTLETYPENO']}','{d[0]['mainTable']['FRECCONDITIONNO']}','{d[0]['mainTable']['FPRICELISTNO']}',getdate(),0,'{d[0]['mainTable']['F_SZSP_Text']}','{d[0]['mainTable']['F_SZSP_KHZYJBNo']}','{d[0]['mainTable']['F_SZSP_KHGHSXNo']}','{d[0]['mainTable']['F_SZSP_XSMSNo']}','{d[0]['mainTable']['F_SZSP_XSMSSXNo']}')"

                    insertData(app2, sql3)
                    print(f"{d[0]['mainTable']['FNameId']}保存到SRC,5分钟后同步到ODS")

                    changeStatus(app2, "1", 'RDS_OA_ODS_bd_CustomerList', "FCustomerName",
                                    (d[0]['mainTable'])['FName2052'])
                else:
                    changeStatus(app2, "2", 'RDS_OA_ODS_bd_CustomerList', "FCustomerName", k['FCustomerName'])
                    print(f"编码{d[0]['mainTable']['FNameId']}已存在于数据库，请手动录入")
            else:
                print(f"该客户不在今日审批中{k['FCustomerName']}")
                changeStatus(app2, "2", 'RDS_OA_ODS_bd_CustomerList', "FCustomerName", k['FCustomerName'])

    sql4 = "select top 1 FInterId,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCOUNTRY,FPROVINCIAL,FTEL,FINVOICETITLE,FTAXREGISTERCODE,FBankName,FINVOICETEL,FAccountNumber,FINVOICEADDRESS,FINVOICETYPE,FTaxRate,FCONTACT,FBizAddress,FMOBILE,FSalesman,FAalesDeptName,FCustTypeNo,FGroupNo,F_SZSP_KHFLNo,FSalesGroupNo,FTRADINGCURRNO,FSETTLETYPENO,FRECCONDITIONNO,FPRICELISTNO,FUploadDate,FIsdo,F_SZSP_Text,F_SZSP_KHZYJBNo,F_SZSP_KHGHSXNo,F_SZSP_XSMSNo,F_SZSP_XSMSSXNo,F_SZSP_BLOCNAME from RDS_OA_ODS_bd_CustomerDetail where FIsdo = 0"
    # sleep(30)
    result = getData(app2, sql4)

    if result:

        api_sdk = K3CloudApiSdk()

        res=ERP_customersave(api_sdk, option1, result, app3,app2)

        return res

    else:

        return "无需要同步的客户"





