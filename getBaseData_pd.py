# -*- coding: utf-8 -*-
from __future__ import division  ###小数除法
default_encoding="utf-8"
import pandas as pd
import os
import numpy as np
import math
from datetime  import datetime

import calendar
from file_path_config import file_path_dict


def getAllControlVariance():
    huanshoulv = geControlVariance("月换手率")
    huanshoulv.columns = ["month", "huanshoulv"]
    shiyinglv = geControlVariance("月市盈率")
    shiyinglv.columns = ["month","shiyinglv"]
    shizhi = geControlVariance("市值")
    shizhi.columns = ["month", "shizhi"]
    ppi = geControlVariance("PPI")
    ppi.columns = ["month", "ppi"]
    cpi = geControlVariance("CPI")
    cpi.columns = ["month", "cpi"]
    xinzenggudingtouzi = geControlVariance("新增固定投资")
    xinzenggudingtouzi.columns = ["month", "xinzenggudingtouzi"]
    gudingtouzi = geControlVariance("固定投资")
    gudingtouzi.columns = ["month", "gudingtouzi"]
    m2 = geControlVariance("M2增长率")
    m2.columns = ["month", "m2"]
    pd_1 = geControlVariance("PD")
    pd_1.columns = ["month", "pd_1"]
    data=pd.merge(huanshoulv, shiyinglv,how="inner",on="month")
    data=pd.merge(data, shizhi,on="month",how="inner")
    data=pd.merge(data, ppi,on="month",how="inner")
    data=pd.merge(data, cpi,on="month",how="inner")
    data=pd.merge(data, xinzenggudingtouzi,on="month",how="inner")
    data=pd.merge(data, gudingtouzi,on="month",how="inner")
    data=pd.merge(data, m2,on="month",how="inner")
    data=pd.merge(data, pd_1,on="month",how="inner")
    data["cpi"]=data["cpi"].shift(1)
    data["month"]=data["month"].astype(int)
    data=data.fillna(0)
    return data



###作为getMonthFiveFF_pd apply的函数
def getmonth(month):
    if int(month[1]) < 10:
        tmp='0'+str(month[1])
    else:
        tmp=str(month[1])
    return str(month[0])+tmp
def getMonthFiveFF_pd():
    """
    月五因子
    :return:
    """
    original_data_pd = pd.read_table(file_path_dict["month_five_ff"],header=0)

    original_data_pd.drop("MarkettypeID",axis=1,inplace=True)
    original_data_pd.drop("Portfolios",axis=1,inplace=True)
    original_data_pd.TradingMonth= original_data_pd.TradingMonth.apply(lambda month :datetime.strptime(month, "%b-%y"))
    original_data_pd.TradingMonth= original_data_pd.TradingMonth.apply(lambda  day: str(day)[:4]+str(day)[5:7])

    original_data_pd.fillna(0)
    original_data_pd.columns=["month","riskpremium1", "smb1", "hml1", "rmv1", "cma1"]
    original_data_pd["month"]= original_data_pd["month"].astype(int)
    return original_data_pd



def getGTAMonthReturn_pd():
    """
    国泰安月收益率
    :return:
    """
    original_data =  pd.read_table(file_path_dict["gta_month_return"],sep="\t",header=0)
    original_data=original_data[["Trdmnt","Mretwd","Mretnd"]]
    original_data.Trdmnt= original_data.Trdmnt.apply(lambda month :month[:4]+month[5:])
    original_data.columns=["month","month_return","month_return_without_fenhong"]
    print(original_data.head(10))
    return original_data



####获取某个目录下的文件名列表
def getFilePathList(path):
    """
    path 路径下所有文件
    :param path:
    :return:
    """
    file_path_list = []
    for root, dirs, files in os.walk(path):
        for filespath in files:
            file_path_list.append(os.path.join(root,filespath))
    return file_path_list


def getGTADayReturn_pd():
    """
    国泰安日收益率
    :return:
    """
    file_path_list = getFilePathList(file_path_dict["gta_day_return_path"])  ###日收益
    all_data = pd.DataFrame(columns=["day","day_return","day_return_without_fenhong"])
    for file_path in file_path_list:
        print(file_path)
        data=pd.read_table(file_path,sep='\t',header=0)
        data=data[["Trddt","Dretwd","Dretnd"]]
        data.Trddt=data.Trddt.apply(lambda day:str(day).replace("-",'')) ###-将破折号去掉
        data.columns=["day","day_return","day_return_without_fenhong"]
        all_data=all_data.append(data) ###返回一个新的df，所以要重新赋值
    print(all_data.head(10))
    return all_data

############作为getDay5ff_pd apply函数
def getday(day):
    day=str(day)
    seperate_day = day.split('-')
    if int(seperate_day[1]) < 10:
        seperate_day[1] = '0' + seperate_day[1]
    if int(seperate_day[2]) < 10:
        seperate_day[2] = '0' + seperate_day[2]
    day = seperate_day[0] + seperate_day[1] + seperate_day[2]
    return day

def getDay5ff_pd():
    """
    日五因子
    :return:
    """
    original_data = pd.read_table("d:/risks/day_ff.txt",header=0)

    original_data.drop("MarkettypeID",axis=1,inplace=True)
    original_data.drop("Portfolios",axis=1,inplace=True)
    original_data.TradingDate= original_data.TradingDate.apply(getday)
    original_data.columns=["day", "riskpremium1", "smb1", "hml1", "raw1", "cma1"]
    original_data["day"]=original_data["day"].astype("str")
    # print(original_data.head(10))
    return original_data

def getDayRiskFreeReturn_pd():
    """
    日无风险利率
    :return:
    """
    original_data = pd.read_table(file_path_dict["day_risk_free"],header=0)
    original_data.date=original_data.date.apply(lambda  day: str(day)[:4]+str(day)[5:7]+str(day)[8:10])
    # print(original_data.head(10))
    return original_data
def getLogDayRiskFreeReturn_pd():
    """
    对数日无风险利率
    :return:
    """
    original_data = pd.read_table(file_path_dict["day_risk_free"],header=0)
    original_data.date=original_data.date.apply(lambda  day: str(day)[:4]+str(day)[5:7]+str(day)[8:10])
    original_data["log_free_risk_return"]= original_data["return"].apply(lambda  item :math.log(item+1))
    original_data.drop("return",axis=1,inplace=True)
    original_data.columns=["day","log_free_risk_return"]
    original_data["day"]=original_data["day"].astype("str")
    # print(original_data.head(10))
    return original_data

def geControlVariance(sheet_name):
    """
    按sheet_name 获得控制变量
    :param sheet_name:
    :return:
    """
    pd_data=pd.read_excel(file_path_dict["control_variance"],sheet_name=sheet_name)
    pd_data.columns=["month","value"]
    pd_data.month=pd_data.month.astype("str")
    pd_data.month=pd_data.month.apply(lambda item :item[0:4]+item[5:7])
    return pd_data


def getMonthRiskFreeReturn_pd():
    """
    月无风险利率
    :return:
    """
    original_data = pd.read_table(file_path_dict["month_risk_free"],header=0)
    original_data.drop("Year",axis=1,inplace=True)
    original_data.drop("Month",axis=1,inplace=True)
    original_data.Date=original_data.Date.apply(lambda  day: str(day)[:4]+str(day)[5:7]+str(day)[8:10])
    original_data.columns=["month","free_risk_return"]
    original_data=original_data.fillna(0)
    return original_data

def getLogMonthRiskFreeReturn_pd():
    """
    对数月无风险利率
    :return:
    """
    original_data = pd.read_table(file_path_dict["month_risk_free"],header=0)
    original_data.drop("Year",axis=1,inplace=True)
    original_data.drop("Month",axis=1,inplace=True)
    original_data.Date=original_data.Date.apply(lambda  day: str(day)[:4]+str(day)[5:7])###得到月份
    original_data.columns=["month","free_risk_return"]
    original_data["month"]=original_data["month"].astype(int) ###默认存储的是int
    original_data["log_free_risk_return"]= original_data["free_risk_return"].apply(lambda  item :math.log(item+1))
    original_data=original_data.fillna(0)
    return original_data




def calcLogDayReturn_pd():
    """
    计算日对数收益率
    :return:
    """
    starttime = datetime.now()
    file_path_list=getFilePathList(file_path_dict["signal_bond_path"])
    day_return = pd.DataFrame(columns=["day","log_diff_return","bond_code"])
    for file_path in file_path_list:
        print(file_path)
        original_data = pd.read_table(file_path,sep=",",header=None)
        original_data=original_data.fillna(0)
        original_data.columns=["bond_code","day","time","price","other"] ###给字段命名
        bond_code=str(int(original_data.ix[0,0])) ###取得bond_code 方便使用
        original_data.drop("other",axis=1,inplace=True)
        original_data["day"]=original_data["day"].astype(str)
        original_data.sort_values(["day","time"],ascending=[1,1],inplace=True)

        original_data["log_price"]=original_data.price.apply(np.log)
        original_data["log_diff_return"]=original_data.log_price.diff()
        original_data=original_data.fillna(0)
        original_data = original_data.groupby(by=['day'])['log_diff_return'].sum()  ###在pandas中估计有更简单的做法，目前的写法是向量化的写法，已经比较简洁
        original_data=pd.DataFrame(original_data)
        original_data=original_data.reset_index()
        original_data["bond_code"]=str(bond_code)
        # print(original_data.head((10)))
        day_return=day_return.append(original_data) ###append后需要返回到新的df
    save_file_path="d:/pandas/"+"day_return.txt"
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path) ###若文件存在，先删除
    day_return.to_csv(save_file_path,mode='a',index=False) #df.to_csv, 参数mode='a'表示追加
    endtime = datetime.now()
    diff_time = (endtime - starttime)
    print("计算时间：", diff_time)

def getLogDayReturn():
    """
    读取收益数据
    :return:
    """
    log_day_return=pd.read_table(file_path_dict[u"day_return"],sep=",",header=0)
    log_day_return["day"]=log_day_return["day"].astype("str") ###确保使用字符串为key，可以使用高级的时间序列，下个版本改进
    return log_day_return

def getLogMonthReturn_pd():
    """
    对数月收益
    :return: 返回月收益
    """
    month_return =  pd.read_table(file_path_dict["month_return"],sep=",",header=0)
    return month_return


def calcLogMonthReturn_pd():
    """
    计算月对数收益率
    :return:
    """
    file_path_list=getFilePathList(file_path_dict["signal_bond_path"])
    month_return = pd.DataFrame(columns=["month","log_diff_return","bond_code"])
    for file_path in file_path_list:
        print(file_path)
        original_data = pd.read_table(file_path,sep=",",header=None)
        original_data=original_data.fillna(0)
        original_data.columns=["bond_code","day","time","price","other"] ###给字段命名
        bond_code=str(int(original_data.ix[0,0])) ###取得bond_code 方便使用
        original_data.drop("other",axis=1,inplace=True)
        original_data["day"]=original_data["day"].astype(str)
        original_data.sort_values(["day","time"],ascending=[1,1],inplace=True)
        original_data["month"] = original_data.day.apply(lambda item: item[0:6])
        original_data["month"] = original_data["month"].astype(str)
        original_data["log_price"]=original_data.price.apply(np.log)
        original_data["log_diff_return"]=original_data.log_price.diff()
        original_data=original_data.fillna(0)
        original_data = original_data.groupby(by=['month'])['log_diff_return'].sum()  ###在pandas中估计有更简单的做法，目前的写法是向量化的写法，已经比较简洁
        original_data=pd.DataFrame(original_data)
        original_data=original_data.reset_index()
        original_data["bond_code"]=str(bond_code)
        # print(original_data.head((10)))
        month_return=month_return.append(original_data) ###append后需要返回到新的df
    save_file_path=file_path_dict["month_return"]
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path) ###若文件存在，先删除
    month_return.to_csv(save_file_path,mode='a',index=False) #df.to_csv, 参数mode='a'表示追加



def getJumpData():
    """
    取得跳跃数据
    :return:
    """
    jump_data=pd.read_table(file_path_dict[u"jump_data"],sep=",",header=0)
    return  jump_data

def calcJumpData():
    """
    计算跳跃
    :return:
    """
    starttime = datetime.now()
    file_path_list = getFilePathList(file_path_dict["signal_bond_path"])
    k=0
    jump_data = pd.DataFrame(columns=["bond_code", "month", "size_rjv", "mean_rjv", "arr_rjv", "std_rjv"])

    for file_path in file_path_list:
        # if k==10:
        #     break
        # k+=1
        print(file_path)
        original_data = pd.read_table(file_path, sep=",", header=None)
        original_data=original_data.fillna(0)
        original_data.columns = ["bond_code", "day", "time", "price", "other"]  ###给字段命名
        bond_code = str(int(original_data.ix[0, 0]))  ###取得bond_code 方便使用
        original_data.drop("other", axis=1, inplace=True)
        original_data["day"] = original_data["day"].astype(str)
        original_data["time"] = original_data["time"].astype(str)
        original_data.sort_values(["day", "time"], ascending=[1, 1], inplace=True)
        original_data["bond_code"] = original_data["bond_code"].astype(str)
        original_data["time"] = original_data["time"].astype(str)
        original_data["log_price"] = original_data.price.apply(np.log)
        original_data["log_diff_return"] = original_data.log_price.diff()
        original_data["log_diff_return_t_1"] = original_data.log_diff_return.shift(1) ###得到vt-1,方便相乘
        original_data["log_diff_return_t_2"] = original_data.log_diff_return.shift(2) ###得到vt-2,方便相乘
        original_data = original_data.fillna(0) ###首先把Nan填充为0，防止意外出错
        original_data["rv"] = original_data.log_diff_return**2
        original_data["bv"] = original_data.log_diff_return*original_data.log_diff_return_t_1
        original_data["rv_bv"]=original_data.rv-original_data.bv ###计算rv_t-bv_t ###以上均为对time求值，还需要算日数据
        original_data["tp"]=((original_data.log_diff_return**4)*\
                            (original_data.log_diff_return_t_1**4)\
                            *(original_data.log_diff_return_t_2**4))**(1/3)  ###直接**4/3有错误，采用先4次方，再开3次方


        day_group_data = original_data.groupby('day') ###日汇总，得到的是groupby 对象
        day_count_data=day_group_data.count() ####求每个分组的个数
        day_sum_data = day_group_data.sum() ####求和
        ######按照具体公式，算指标
        ###计算bv
        day_sum_data["bv"]=(math.pi/2)*day_sum_data["bv"]*day_count_data["bv"]/(day_count_data["bv"]-1)

        ###计算tp
        alpha1=1/(4*(math.gamma(7/6)/math.gamma(1/2))**3)
        day_sum_data["tp"]=alpha1*(day_count_data["tp"]*day_count_data["tp"]/(day_count_data["tp"]-2))*day_sum_data["tp"]
        day_sum_data=day_sum_data.fillna(0)  ###做过除法后若有nan将nan设为0
        ###计算Z
        day_sum_data["tp_bv"]=day_sum_data["tp"]/(day_sum_data["bv"]**2)###先计算 tp/bv**2
        day_sum_data=day_sum_data.fillna(0)  ###做过除法后若有nan将nan设为0
        day_sum_data["tp_bv"]=day_sum_data["tp_bv"].apply(lambda  item : 1 if item<1 else item ) ###求max(1,tp/bv**2)
        alpha2=1/np.power(math.pi**2+math.pi-5,0.5)
        day_sum_data["Z"]=alpha2*(day_count_data["rv_bv"]**0.5)*(day_sum_data["rv_bv"]/day_sum_data["rv"])/(day_sum_data["tp_bv"]**0.5)
        day_sum_data=day_sum_data.fillna(0)  ###做过除法后若有nan将nan设为0

        day_sum_data["Z_normal"]=day_sum_data["Z"].apply(lambda  item :1 if item>=1.96 else 0) ###做出判断条件
        day_sum_data["rjv"]=day_sum_data["Z_normal"]*np.abs(day_sum_data["rv_bv"])**0.5

        ###增加两列 month

        day_sum_data=day_sum_data.reset_index() ##取消索引，得到day， 高级做法用时间序列，下一个版本改进
        day_sum_data["month"] = day_sum_data["day"].apply(lambda item: item[0:6])
        day_sum_data["month"] = day_sum_data["month"].astype(str) ###月份，方便统计，在pandas中有时间高级用法；目前这么写也很简便

        month_group_data=day_sum_data.groupby("month")
        month_std_data=month_group_data.std()
        month_sum_data=month_group_data.sum()
        month_sum_data["std_rjv"]=month_std_data["rjv"]
        month_sum_data = month_sum_data.reset_index() ###将index转为字段 ###两个聚合结果做运算需要index，所以，std_rjv要在释放index前做，下面其实有直接用index的方法，不需要释放，
        month_sum_data["month"]= month_sum_data["month"].astype(str)

        month_sum_data["size_rjv"]=month_sum_data["rjv"]
        month_sum_data["mean_rjv"]=month_sum_data["rjv"]/month_sum_data["Z_normal"]

        month_sum_data=month_sum_data.fillna(0)  ###做过除法后若有nan将nan设为0
        month_sum_data["arr_rjv"]=month_sum_data["size_rjv"]/month_sum_data["month"].\
            apply(lambda  item : calendar.monthrange(datetime.strptime(item,"%Y%m").year,
                                                     datetime.strptime(item,"%Y%m").month)[1]) ###apply 里是lambda表达式 计算当月多少天
        month_sum_data=month_sum_data.fillna(0)
        month_sum_data["bond_code"] = bond_code ###将bond_code加上
        month_sum_data=month_sum_data[["bond_code","month","size_rjv","mean_rjv","arr_rjv","std_rjv"]]
        jump_data=jump_data.append(month_sum_data) ###append函数返回新的df

    save_file_path=file_path_dict["jump_data"]
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path) ###若文件存在，先删除
    jump_data.to_csv(save_file_path,mode='a',index=False) #df.to_csv, 参数mode='a'表示追加
    endtime = datetime.now()
    diff_time=(endtime-starttime)
    print("计算时间：",diff_time)


def test():
    file_path_list=getFilePathList("d:/result_new")
    month_return = pd.DataFrame(columns=["month","log_diff_return","bond_code"])
    for file_path in file_path_list:
        print(file_path)
        original_data = pd.read_table(file_path,sep=",",header=None)
        original_data.fillna(0)
        original_data.columns=["bond_code","day","time","price","other"] ###给字段命名
        bond_code=str(int(original_data.ix[0,0])) ###取得bond_code 方便使用
        original_data.drop("other",axis=1,inplace=True)
        original_data["day"]=original_data["day"].astype(str)
        original_data["time"]=original_data["time"].astype(str)
        original_data.sort_values(["day","time"],ascending=[1,1],inplace=True)

        save_file_path="d:/pandas/"+"test.txt"

        original_data.to_csv(save_file_path,mode='a',index=False) #df.to_csv, 参数mode='a'表示追加



def calcMonthIvData():
    """
    计算月iv所需数据
    :return:
    """
    log_day_return=getLogDayReturn()
    log_day_risk_free_return=getLogDayRiskFreeReturn_pd()
    # print(log_day_risk_free_return)
    day_five_ff = getDay5ff_pd()
    day_return=pd.merge(log_day_return,log_day_risk_free_return,how="inner",on="day") ###拼接量数据
    day_return.dropna(axis=0, how='all') ###合并时删除未空的行，如果上面的merge how="inner"就没有空的问题
    data=pd.merge(day_return,day_five_ff,how="inner",on="day")
    day_return.dropna(axis=0, how='all')  ###合并时删除未空的行，如果上面的merge how="inner"就没有空的问题
    save_file_path=file_path_dict["month_iv_data"]
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path)  ###若文件存在，先删除
    data.to_csv(save_file_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加

def getMonthIVData():
    """
    计算month_iv所需数据
    :return:
    """
    month_iv_data = pd.read_table(file_path_dict[u"month_iv_data"], sep=",", header=0)
    month_iv_data["day"] = month_iv_data["day"].astype("str")  ###确保使用字符串为key，可以使用高级的时间序列，下个版本改进
    month_iv_data=month_iv_data.dropna(axis=0, how='all') ###dropna返回一个df
    month_iv_data=month_iv_data.fillna(0)
    return month_iv_data

def getMonthIV():
     iv = pd.read_table(file_path_dict[u"month_iv"], sep=",", header=0)
     iv.columns=["bond_code","month","error","iv"]
     iv = iv.dropna(axis=0, how='all')  ###dropna返回一个df ###删除空行
     iv = iv.fillna(0)
     return  iv





def calcFirstRegressionData():
    """
    计算第一步回归所需数据
    :return:
    """
    jump_data=getJumpData()
    month_five_ff=getMonthFiveFF_pd()
    log_month_return=getLogMonthReturn_pd()
    log_month_risk_free_return=getLogMonthRiskFreeReturn_pd()
    month_iv=getMonthIV()
    control_variances=getAllControlVariance()

    data=pd.merge(jump_data,month_iv,how="inner",on=["bond_code","month"])

    data=pd.merge(data,log_month_return,how="inner",on=["bond_code","month"])
    data=pd.merge(data,log_month_risk_free_return,how="inner",on=["month"])
    data=pd.merge(data,month_five_ff,how="inner",on=["month"])
    data=pd.merge(data,control_variances,how="inner",on=["month"])
    data["r_rft"]=data["log_diff_return"]-data["free_risk_return"]###超额收益率
    data["iv_square"]=data["iv"]**2
    data["size_rjv_square"]=data["size_rjv"]*data["size_rjv"]
    data["mean_rjv_square"]=data["mean_rjv"]*data["mean_rjv"]
    data["arr_rjv_square"]=data["arr_rjv"]*data["arr_rjv"]
    data["std_rjv_square"]=data["std_rjv"]*data["std_rjv"]
    data["iv_size_rjv"]=data["iv"]*data["size_rjv"]
    data["iv_mean_rjv"]=data["iv"]*data["mean_rjv"]
    data["iv_arr_rjv"]=data["iv"]*data["arr_rjv"]
    data["iv_std_rjv"]=data["iv"]*data["std_rjv"]
    #
    data = data.dropna(axis=0, how='all')  ###dropna返回一个df
    data = data.fillna(0)
    save_file_path=file_path_dict["first_regression_data"]
    # if os.access(save_file_path, os.F_OK):
    #     os.remove(save_file_path)  ###若文件存在，先删除
    # data.to_csv(save_file_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加
    return data


def getFirstRegressionData():
    """
    返回第一步回归所需数据
    :return:
    """
    calcFirstRegressionData = pd.read_table(file_path_dict[u"first_regression_data"], sep=",", header=0)
    return calcFirstRegressionData



def getFirstRegressionCoef():
    """
    返回第一步回归的计算结果
    :return:
    """
    first_regression_coef = pd.read_table(file_path_dict[u"first_regression_coef"], sep=",", header=0)
    return first_regression_coef

def calcSecondRegressionData():
    """
    计算第二步回归所需系数
    :return:
    """
    data=getFirstRegressionCoef()
    log_month_return=getLogMonthReturn_pd()
    log_month_risk_free_return=getLogMonthRiskFreeReturn_pd()


    data = pd.merge(log_month_return, data, how="inner", on=["bond_code"]) ###
    data = pd.merge(data, log_month_risk_free_return, how="inner", on=["month"])
    data["r_rft"]=data["log_diff_return"]-data["free_risk_return"]###超额收益率

    data = data.dropna(axis=0, how='all')  ###dropna返回一个df
    data = data.fillna(0)
    save_file_path=file_path_dict["second_regression_data"]
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path)  ###若文件存在，先删除
    data.to_csv(save_file_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加
    return data

def getSecondRegressionData():
    """
    返回第二步回归所需数据
    :return:
    """
    second_regression_data = pd.read_table(file_path_dict[u"second_regression_data"], sep=",", header=0)
    return second_regression_data

if __name__ == "__main__":
    # data=getLogDayRiskFreeReturn_pd()
    # calcFirstRegressionData()
    # data=getFirstRegressionData()
    calcFirstRegressionData()
    # calcSecondRegressionData()
    # print(data.head(1000))






