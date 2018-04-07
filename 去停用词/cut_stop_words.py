# -*- coding:utf-8 -*-
import jieba  # 导入jieba模块
import pymysql
import re

jieba.load_userdict("my_words_dict.txt")  # 加载自定义词典

from_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='tbs_data', charset='utf8')
from_cursor = from_connect.cursor()

to_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8')
to_cursor = to_connect.cursor()

extract_sql = 'select * from %s'
insert_sql = 'insert into %s values("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", ' \
             '"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", ' \
             '"%s")'
update_sql = 'update %s set extract_content="%s" where id="%s"'


def split_sentence(input_source_table, update_table_name):
    label = set()

    # 把停用词做成字典
    stopwords = {}
    file_stop = open('stop_words.txt', 'r')
    for eachWord in file_stop:
        stopwords[eachWord.strip()] = eachWord.strip()
    file_stop.close()
    # print(stopwords)

    jieba.enable_parallel(4)  # 并行分词

    to_cursor.execute(extract_sql % input_source_table)
    source_data = to_cursor.fetchall()

    # 取出已有的label
    to_cursor.execute('select label from label_type2')
    old_label_type = to_cursor.fetchall()
    old_label_list = []
    for old_label in old_label_type:
        old_label_list.append(old_label[0])
    old_label_list = set(old_label_list)
    print("old_label_list!!!!!!!!!!!!", old_label_list)

    for eachLine in source_data:
        content = eachLine[2]
        line = content.strip()  # 去除每行首尾可能出现的空格，并转为Unicode进行处理
        line1 = re.sub(r"[0-9\s+\.\!\/_,$%^*()?;；:-【】\"\']+|[+—！，;:。？、~@#￥%…&*（）]+", "", line)
        word_list = list(jieba.cut(line1))  # 用结巴分词，对每行内容进行分词
        result_string = []

        for word in word_list:
            if len(word) < 2:
                continue

            marry_count = 0
            for character in word:
                if character in stopwords:
                    marry_count += 1
            # print(marry_count / len(word))
            if marry_count / len(word) > 0.75:
                continue

            if word not in stopwords:
                result_string.append(word)
            if word not in label and word not in old_label_list:
                label.add(word)
        to_cursor.execute(update_sql % (input_source_table, result_string, eachLine[0]))
        print(result_string)

    for label_i in label:
        print('insert into %s VALUES("%s", NULL)' % (update_table_name + "_label_type", label_i))
        to_cursor.execute('insert into %s VALUES("%s", NULL)' % (update_table_name+"_label_type", label_i))
    to_connect.commit()


split_sentence("yanjing_raw_data", "yanjing_baiwei")
split_sentence("baiwei_raw_data", "yanjing_baiwei")
# split_sentence('asahi', "asahi_raw_data")
# split_sentence('kylin', "kylin_raw_data")
# split_sentence("qingdao", "qingdao_raw_data")
# split_sentence("snow", "snow_raw_data")
# split_sentence("panda", "panda_raw_data")
