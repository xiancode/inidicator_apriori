#!/usr/bin/env  python
#-*-coding=utf-8-*-
'''
Description : statistic indicator rules mining  of the Apriori Algorithm
require     :pandas-0.16.2-py2.7
author      : shizhongxian@126.com
usage  $python indicator_apriori.py  -f jck_table.txt  -s 0.10 -c 0.10 -b small
'''
import sys
import pandas as pd
import ConfigParser
import logging
from optparse import OptionParser
import apriori

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='apriori.log',
                filemode='a')

global buckets_dicts

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

def delete_empty_month(df,indicators):
    '''
    删除指标数据不全的月份
    '''
    inter_set = set()
    months_list = []
    for indicator in indicators:
        indi_data = df[df.indicator.isin([indicator])]
        months = set(indi_data["month"].tolist())
        months_list.append(months)
    inter_set = set(months_list[0])
    for months in months_list:
        inter_set = inter_set.intersection(months)
    inter_list = list(inter_set)
    return df[df.month.isin(inter_list)]

def comb_str(list_one,list_two):
    result = []
    if len(list_one) == len(list_two):
        for i in range(len(list_one)):
            result.append(str(list_one[i]) + "_" + str(list_two[i]))
        return result
    else:
        return None

def indicator_classify(datafile,buckets_cls):
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
        
    def get_flag(value):
        for key in buckets_dicts.keys():
            if key[0]<=value and value <= key[1]:
                return buckets_dicts[key]
        return None    
    
    #载入数据
    data = pd.read_table(datafile)
    #列名重命名
    data = data.rename(columns={'时间顺序排序':'month', '地区':'area', 
                                '正式指标':'indicator', '正式数值':'value', '正式单位':'unit'})
    #所有指标
    indicators = data.indicator.unique()
    #过滤掉月份数据不足的指标数据
    data = delete_empty_month(data, indicators)
    print "calculate indicator num ratio"
    con_list = []
    iterr = 0
    for indicator in indicators:
        ratio_data = None
        iterr += 1
        if iterr % 200 == 0:
            print iterr
        #print indicator
        indi_data = data[data.indicator.isin([indicator])]
        #sort_data =  indi_data.sort(columns='month')
        sort_data =  indi_data
        unit_set = set(sort_data["unit"].tolist())
        #过滤掉单位不统一的指标数据
        if len(unit_set) != 1:
            #print "unit error"
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
    #保存第一个时间点
    start_time = months_list[0]
    flag_list = all_data["flag"].tolist()
    com_list = comb_str(months_list,flag_list)
    com_list
    all_data['comb_str'] = pd.Series(com_list)

#     with open("four_year_indicators.txt","w") as fi:
#         caled_indicators = all_data.indicator.unique()
#         for tmp_ in caled_indicators:
#             fi.write(tmp_+"\n")
    
    #转化成  月份_标识符---->[指标1,指标2,.....]形式
    indi_dict = all_data['indicator'].to_dict()
    flag_dict =all_data['comb_str'].to_dict()
    flag_indi_dict = {}
    print "Transform data"
    for key,indicator in indi_dict.iteritems():
        if flag_dict.has_key(key):
            flag = flag_dict[key]
            #去掉第一个时间段，因为第一个时间段所有指标环比都默认为1
            if flag.find(start_time) != -1:
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
    return flag_indi_dict.values()
            
if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-f', '--inputFile',
                         dest='input',
                         help='filename containing csv convert from rec',
                         default=None)
    optparser.add_option('-s', '--minSupport',
                         dest='minS',
                         help='minimum support value',
                         default=0.15,
                         type='float')
    optparser.add_option('-c', '--minConfidence',
                         dest='minC',
                         help='minimum confidence value',
                         default=0.3,
                         type='float')
    optparser.add_option('-b', '--classInterval',
                         dest='buckets_cls',
                         help='gradient interval class',
                         default='small')
    
    (options, args) = optparser.parse_args()
    
    inFile = None
    if options.input is None:
            inFile = sys.stdin
            #inFile = "INTEGRATED-DATASET.csv"
    elif options.input is not None:
            inFile = options.input
    else:
            print 'No dataset filename specified, system with exit\n'
            sys.exit('System will exit')
    
    minSupport = options.minS
    minConfidence = options.minC
    buckets_cls = options.buckets_cls
    #C# 调用
#     def C_shape(inFile,buckets_cls,minSupport, minConfidence):
#         '''
#         C_shape 
#         '''
#         apri_indi_set = indicator_classify(inFile,buckets_cls)
#         inFile = apriori.dataFromList(apri_indi_set)
#         items, rules = apriori.runApriori(inFile, minSupport, minConfidence)
#         apriori.printResults(items, rules)
        
    
    
    apri_indi_set = indicator_classify("test.txt",buckets_cls)
    #apri_indi_set = indicator_classify(inFile,buckets_cls)
    print "excute apriori algorithm"
    inFile = apriori.dataFromList(apri_indi_set)
    items, rules = apriori.runApriori(inFile, minSupport, minConfidence)
    apriori.printResults(items, rules)
    print "End!!"
    
    


