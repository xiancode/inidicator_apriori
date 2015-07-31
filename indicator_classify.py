#!/usr/bin/env  python
#-*-coding=utf-8-*-
#author shizhongxian@126.com

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ConfigParser
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='apriori.log',
                filemode='a')

global buckets
global ranks
global buckets
global buckets_dicts

#
buckets = [[ float('-Inf'),-1.0],[ -1.0,-0.8],[ -0.8,-0.6],[ -0.6,-0.4],
               [ -0.4,-0.2],[ -0.2,0.0],[ 0.0,0.2],[ 0.2,0.4],[ 0.4,0.6],
               [ 0.6,0.8],[ 0.8,1.0],[ 1.0,float('Inf')]]
ranks = list('ABCDEFGHIJKL')

#
#buckets = [[ float('-Inf'),-1.0],[ -1.0,-0.5],[ -0.5,0.0],[ 0.0,0.5],[ 0.5,1.0],[ 1.0,float('Inf')]]
#ranks = list('ABCDEF')

#产生比值区间及标识符设定
buckets_dict = {}
for i in range(len(buckets)):
    key = tuple(buckets[i])
    value = ranks[i]
    buckets_dict[key] = value

def get_flag(value):
        for key in buckets_dict.keys():
            if key[0]<=value and value <= key[1]:
                return buckets_dict[key]
        return None

#求环比
def hb_ratio(tmp_list):
    '''
   求环比 
    '''
    ratio_list=[]
    ratio_list.append(1.0)
    for i in range(1,len(tmp_list)):
        #print i,":",(tmp_list[i]-tmp_list[i-1])/tmp_list[i-1]
        ratio_list.append(round((tmp_list[i]-tmp_list[i-1])/tmp_list[i-1],2))
    return ratio_list

def comb_str(list_one,list_two):
    result = []
    if len(list_one) == len(list_two):
        for i in range(len(list_one)):
            result.append(str(list_one[i]) + "_" + str(list_two[i]))
        return result
    else:
        return None

def indicator_classify(datafile,months=48,buckets_cls='small'):
    '''
    计算指标时间序列变动，根据变动范围对指标归类
    传入文件包括 '月份顺序排序, '地区',  '正式指标', '正式数值', '正式单位'
    '''
    cf = ConfigParser.ConfigParser()
    cf.read('apriori.cfg')
    #get buckets
    try:
        buckets_dicts = eval(cf.get('buckets_dicts', buckets_cls))
    except Exception,e:
        print  "配置文件中buckets_dicts 获取失败",e
        
    #载入数据
    data = pd.read_table(datafile)
    #列名重命名
    data = data.rename(columns={'月份顺序排序':'month', '地区':'area', 
                                '正式指标':'indicator', '正式数值':'value', '正式单位':'unit'})
    #所有指标
    indicators = data.indicator.unique()
    print "calculate indicator num ratio"
    con_list = []
    iterr = 0
    for indicator in indicators:
        iterr += 1
        if iterr % 200 == 0:
            print iterr
        #print indicator
        indi_data = data[data.indicator.isin([indicator])]
        sort_data =  indi_data.sort(columns='month')
        months_list = sort_data["month"].tolist()
        unit_set = set(sort_data["unit"].tolist())
        #过滤掉单位不统一的指标数据
        if len(unit_set) != 1:
            #print "unit error"
            continue
        #过滤掉月份数据不足的指标数据
        if len(months_list) != months:
            #print 'month error',len(months_list)
            #print indicator
            continue
        nums_list = sort_data["value"].tolist()
        ratio_list = hb_ratio(nums_list)
        ratio_data = sort_data.set_index(sort_data.month)
        #添加环比数据
        ratio_data['ration'] = pd.Series(ratio_list,index=ratio_data.month)
        flag_list = map(get_flag,ratio_list)
        ratio_data['flag'] = pd.Series(flag_list,index=ratio_data.month)
        #print ratio_data
        con_list.append(ratio_data)
    print "concat data ..."
    all_data = pd.concat(con_list,ignore_index=True)
    #形成新的列  年/月份_标识符   201101_K 的样式  
    months_list = all_data["month"].tolist()
    flag_list = all_data["flag"].tolist()
    com_list = comb_str(months_list,flag_list)
    com_list
    all_data['comb_str'] = pd.Series(com_list)
    
    #
    with open("four_year_indicators.txt","w") as fi:
        caled_indicators = all_data.indicator.unique()
        for tmp_ in caled_indicators:
            fi.write(tmp_+"\n")
    
    #转化成  月份_标识符---->[指标1,指标2,.....]形式
    indi_dict = all_data['indicator'].to_dict()
    flag_dict =all_data['comb_str'].to_dict()
    flag_indi_dict = {}
    print "Transform data"
    for key,indicator in indi_dict.iteritems():
        if flag_dict.has_key(key):
            flag = flag_dict[key]
            if flag.find('201101') != -1:
                continue
            if flag_indi_dict.has_key(flag):
                flag_indi_dict[flag].append(indicator)
            else:
                flag_indi_dict[flag] = []
                flag_indi_dict[flag].append(indicator)
    #保存结果      
    print "Save data..."      
    with open("Apriori_indicators.txt","w") as f:
        line_no = 0
        for k,value_list in flag_indi_dict.iteritems():
            line_no += 1
            if line_no%100 == 0:
                print line_no
            #f.write(k+"\t")
            f.write('\t'.join(value_list))
            f.write("\n")
            
if __name__ == "__main__":
    #indicator_classify("2011_2014_single_month_table.txt")
    indicator_classify("jck_table.txt",months=48,buckets_cls='small')
    print "End!!"
    
    


