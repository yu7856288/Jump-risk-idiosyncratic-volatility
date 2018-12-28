# -*- coding: utf-8 -*-
from __future__ import division  ###小数除法
default_encoding="utf-8"
from getBaseData_pd import *
import statsmodels.api as sm
import logging

def calcOLS_iv(data,y_var,x_vars):
    Y=data[y_var]
    X=data[x_vars]
    X = sm.add_constant(X)  ###加一个常数项
    est=sm.OLS(Y,X,missing='drop').fit()
    # est=sm.OLS(Y,X,missing='drop').fit(cov_type='HAC',cov_kwds={'maxlags':3})
    if len(est.params)==(len(x_vars)+1): ###回归时出现少于自变量加常数项的个数数，导致无法组合成df，组成series，过滤不符合要求的组
        return est.params

def calcOLS(data,y_var,x_vars):
    Y=data[y_var]
    X=data[x_vars]
    X = sm.add_constant(X)  ###加一个常数项
    est=sm.OLS(Y,X,missing='drop').fit()
    # est=sm.OLS(Y,X,missing='drop').fit(cov_type='HAC',cov_kwds={'maxlags':3})
    if len(est.params)==(len(x_vars)+1): ###回归时出现少于自变量加常数项的个数数，导致无法组合成df，组成series，过滤不符合要求的组
        return est.params
def calcOLS2(data,y_var,x_vars):
    Y=data[y_var]
    X=data[x_vars]
    X = sm.add_constant(X)  ###加一个常数项
    est=sm.OLS(Y,X,missing='drop').fit()
    # est=sm.OLS(Y,X,missing='drop').fit(cov_type='HAC',cov_kwds={'maxlags':3})
    if len(est.params)==(len(x_vars)+1): ###回归时出现少于自变量加常数项的个数数，导致无法组合成df，组成series，过滤不符合要求的组
        print(est.summary())
        return est.params

def calcMonthIV():
    MonthIVData=getMonthIVData()
    MonthIVData["dt_drft"]=MonthIVData["log_diff_return"]-MonthIVData["log_free_risk_return"]

    y_var="dt_drft"
    x_vars=["riskpremium1","smb1","hml1","raw1","cma1"]

    # print(first_gression_data[first_gression_data.isnull().values==True])
    group_first_gression_data=MonthIVData.groupby("bond_code")
    result_data = group_first_gression_data.apply(calcOLS_iv, y_var, x_vars)
    result_data = result_data.reset_index()
    data=pd.merge(MonthIVData,result_data,on="bond_code",how="inner",suffixes=('_data', '_coef')) ###接下来计算日iv，statmodel可能有更简单的方法，下个版本改进
    data["error"]=(data["dt_drft"]-(data["riskpremium1_data"]*data["riskpremium1_coef"]+
                                   data["smb1_data"] * data["smb1_coef"]+
                                   data["hml1_data"] * data["hml1_coef"]+
                                   data["raw1_data"] * data["raw1_coef"]+
                                   data["raw1_data"] * data["raw1_coef"]
                                   ))**2  ###求每日残差平方
    data["month"]=data["day"].apply(lambda  item :item[:6]) ###提取月份
    group_data=data.groupby(["bond_code","month"])[["error"]] ###按bon_code,month 分组
    count_group_data=group_data.count()
    sum_group_data=group_data.sum()
    sum_group_data["month_iv"]=(sum_group_data["error"]/count_group_data["error"])**0.5  ###均方差即为month_iv
    sum_group_data=sum_group_data.reset_index()
    logger.info(sum_group_data)
    file_save_path=file_path_dict[u"month_iv"]
    if os.access(file_save_path, os.F_OK):
        os.remove(file_save_path)  ###若文件存在，先删除
    sum_group_data.to_csv(file_save_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加



def calcFirstRegression(y_var,x_vars):
    first_regression_data=getFirstRegressionData()
    group_first_regression_data=first_regression_data.groupby("bond_code")
    # print(group_first_regression_data.count())
    # print(group_first_regression_data.dtypes)
    result_data=group_first_regression_data.apply(calcOLS,y_var,x_vars)
    result_data=result_data.reset_index()
    save_file_path = file_path_dict["first_regression_coef"]
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path)  ###若文件存在，先删除
    result_data.to_csv(save_file_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加


def calcSecondRegression(y_var,x_vars,output_file_coef,output_file_result):
    second_regression_data=getSecondRegressionData()
    group_Second_regression_data=second_regression_data.groupby("month").mean()
    result_data=group_Second_regression_data.apply(calcOLS2,y_var,x_vars)
    result_data=result_data.reset_index()
    save_file_path = output_file_coef
    if os.access(save_file_path, os.F_OK):
        os.remove(save_file_path)  ###若文件存在，先删除
    result_data.to_csv(save_file_path, mode='a', index=False)  # df.to_csv, 参数mode='a'表示追加
    #### t 检验，statsmodel可能有，没找到
    mean_result_data=result_data.mean()
    std_result_data=result_data.std()
    count_result_data=result_data.count()
    t_value=mean_result_data/(std_result_data/(count_result_data-1)**0.5)

    mean_coef=mean_result_data
    result=pd.DataFrame([t_value,mean_coef])
    result=result.T
    result.columns=['t_value','mean_coef']
    logger.info(result)
    result.to_excel(output_file_result)
    print(result)


if __name__ == "__main__":
    y_var="r_rft"
    x_vars=[
            "size_rjv",
            "mean_rjv",
            "arr_rjv",
            "std_rjv",
            # "riskpremium1",
            "smb1",
            "hml1",
            "rmv1",
            "cma1",
            "iv",
            # "size_rjv_square",
            # "mean_rjv_square",
            # "arr_rjv_square",
            # "std_rjv_square",

            "iv_square",
            "iv_size_rjv",
            "iv_mean_rjv",
            "iv_arr_rjv",
            "iv_std_rjv",
            # "huanshoulv",
            # "shiyinglv",
            # "shizhi",
            # "ppi",
            # "cpi",
            # "xinzenggudingtouzi",
            # "gudingtouzi",
            # "m2",
            # "pd_1",
            ]
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler('表三.log')
    logger.addHandler(handler)
    second_regression_coef_path="d:/pandas/result/跳跃不含iv_pd_coef.txt"
    second_regression_result="d:/pandas/result/跳跃含不iv_pd_coef.xls"

    calcFirstRegression(y_var,x_vars)
    calcSecondRegressionData() ###根据第一步的系数计算第二步所需数据
    calcSecondRegression(y_var,x_vars,second_regression_coef_path,second_regression_result)
    logger.info("########################################################################################")
    logger.info("########################################################################################")




