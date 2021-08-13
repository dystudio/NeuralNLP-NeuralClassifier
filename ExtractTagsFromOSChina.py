#coding=utf-8
# 如果需要使用北大pkuseg分词，通过如下命令安装python包
# !pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pkuseg

"""
数据预处理功能
根据现有的数据，从开源中国的标签库取得IT相关的标签，更新到原有数据行的新字段
0）从Greenplum读取所有的datarow
1) source: datarow + 文本文件（content）path：~/minio/source/xxxx.txt
2）根据文章内容取得一个关键字集合
3）根据关键字集合和开源中国标签库筛选出一个IT相关的标签集合
4) 插入这个标签集合结果到数据库表的一个字段
author：dystudio
date:20210813
"""

import codecs
import sys
import json
import re
import threading
from time import ctime,sleep
import os
import macropodus
import time
import psycopg2

label_separator = "|"
input_path ="minio/"
source_files_path ="minio/source/"
output_files_path ="minio/output/"
dataset = []
keywords_dataset= []
keywords_string_dataset = []
def pgsql():
    ## 连接到一个给定的数据库
    conn = psycopg2.connect(database="poc_db", user="gpadmin",
                            password="admin", host="172.16.103.36", port="5432")
    print ("connected successfully")
    ## 建立游标，用来执行数据库操作
    cursor = conn.cursor()

    ## 执行SQL命令
    #cursor.execute("DROP TABLE test_conn")
    #cursor.execute("CREATE TABLE test_conn(id int, name text)")
    #cursor.execute("INSERT INTO test_conn values(1,'haha')")
    ## 提交SQL命令
    conn.commit()
    ## 执行SQL SELECT命令
    cursor.execute("SELECT article_url, article_title, article_content_path, article_tags, create_date FROM public.it_xueyuan_article_info_test")

    ## 获取SELECT返回的元组
    rows = cursor.fetchall()
    
    print('total rows count = ',cursor.rowcount)

    for row in rows:
        print('article_url = ',row[0], 'article_title = ', row[1], 'article_content_path = ', row[2], 'article_tags = ', row[3], 'create_date = ', row[4])
        dataset.append(row)
    ## 关闭游标
    cursor.close()
    ## 关闭数据库连接
    conn.close()
def queryKeywords(original_keywords):
    ## 连接到一个给定的数据库
    conn = psycopg2.connect(database="poc_db", user="gpadmin",
                            password="admin", host="172.16.103.36", port="5432")
    print ("connected successfully")
    print("original_keywords:", original_keywords)
    ## 建立游标，用来执行数据库操作
    cursor = conn.cursor()

    ## 执行SQL命令
    #cursor.execute("DROP TABLE test_conn")
    #cursor.execute("CREATE TABLE test_conn(id int, name text)")
    #cursor.execute("INSERT INTO test_conn values(1,'haha')")
    ## 提交SQL命令
    conn.commit()
    ## 执行SQL SELECT命令
    #original_keywords :[(0.05081850022004261, '索引'), (0.04392007537641511, '序列'), (0.040270072582238775, '类型'))]
    #parse original keywords to :  '%SQLyog%', '%Apache%', '%青岛%'
    #SELECT id, tag1 FROM public.oschina_edit WHERE tag1 like any (array['%SQLyog%', '%Apache%', '%青岛%']);
    #"the name is {name_} \nthe age is {age_}".format(name_=name ,age_= age)
    parameterString = ''
    for word in original_keywords:
        print(word[1])
        parameterString += '\'%' + word[1] +  '%\',' 
    parameterString = parameterString[:-1]
    print("parameterString:" , parameterString)
    sql = "SELECT id, tag1 FROM public.oschina_edit WHERE tag1 like any (array[{parameter_}]) ".format(parameter_ = parameterString)
    print('sql',sql)
    cursor.execute(sql)

    ## 获取SELECT返回的元组
    rows = cursor.fetchall()
    
    print('total rows count for keywords = ',cursor.rowcount)

    for row in rows:
        print(row[0], row[1])
        keywords_dataset.append(row)
    ## 关闭游标
    cursor.close()
    ## 关闭数据库连接
    conn.close()
def data_process(df, outfile, tokenize_strategy):
    print('""""""""""data_process start"""""""""""""')
    processId = os.getpid()
    threadId = threading.currentThread().ident
    print(u'%s----------号进程任务----------'%processId)
    print(u'%s----------号线程任务----------'%threadId)

    pgsql()

    with open(outfile, "w+", buffering=20, encoding='utf-8') as output:
        count=0
        print('..........................................')
        time.sleep(3)
        print('--------------dataset start---------------')
        for x in dataset:
            count+=1
            print('--------------datarow start---------------')
            #txt file path:source_files_path + x[2], 读取这条数据对应的文本内容
            original_keywords = []
            content_file_path = source_files_path + x[2]
            with open(content_file_path, "r", encoding='utf-8') as f:  # 打开文件
                content = f.read()  # 读取文件
                original_keywords = macropodus.keyword(content) #keyword object example: [(0.05081850022004261, '索引'), (0.04392007537641511, '序列'), (0.040270072582238775, '类型'), (0.028289520196832764, 'Py')]
                print("----------------keywords start----------------\n", original_keywords, "\n----------------keywords end------------------")
            #根据keywords去查询出开源中国的IT标签库里的标签集合
            queryKeywords(original_keywords)

            for k in keywords_dataset:
                print(k)
                keywords_string_dataset.append(k[1])

            tag = x[3].replace("@#gzg#@","|")
            tags = tag.split("|")
            for t in tags:
                print('tag ',t)
                keywords_string_dataset.append(t)
            #根据当前数据行取得 aiticle_tags + joined_keywords
            #article_url =  https://article.itxueyuan.com/g9wROR article_title =  10数据结构 article_content_path =  g9wROR.txt article_tags =  算法@#gzg#@数据结构 create_date =  2021-07-28
            
            joined_keywords = '|'.join(keywords_string_dataset)
            print('joined_keywords',joined_keywords)
            # joined_keywords,article_title, example: joined_keywords,x[1]
            data_line = joined_keywords + ',' + x[1]
            print('data_line: ', data_line)
            output.write('%s\n' % data_line)  
            print('--------------datarow end-----------------')
            
            print('--------------dataset end-----------------')
          
        print('----------------data_process end---------------')   
    # with open(outfile, "w+", buffering=20, encoding='utf-8') as f:
    #     count=0
    #     for indexs in [1,2]:
    #         count+=1
    #         print(indexs)
    #         json_str = json.dumps(indexs)
    #         # 已添加的方式写入json文件
    #         f.write('%s\n' % json_str)      
    #     print('----------------data_process end---------------')    
            

threads = []
t1 = threading.Thread(target=data_process,args=(any, input_path + 'data.txt', "jieba"))
threads.append(t1)

if __name__ == '__main__':
    for t in threads:
        t.setDaemon(True)
        t.start()
    
    for t in threads:
         t.join() 

    print("all over %s" %ctime())