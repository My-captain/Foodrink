# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/29 下午3:07
# @Author  : Mr.Robot
# @File    : combine_label_type.py
# ==========================================
import pymysql

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink',
                          charset='utf8mb4')
cursor = connect.cursor()


def combine(table_name):
    cursor.execute('select * from %s' % table_name)
    label_list = []
    label_type_result = cursor.fetchall()
    for row in label_type_result:
        label_list.append(row[0])
    label_list = set(label_list)
    cursor.execute('select * from label_type')
    origin_result = cursor.fetchall()
    origin_list = []
    for row in origin_result:
        origin_list.append(row[0])
    origin_list = set(origin_list)
    # print(label_list)
    # print(origin_list)
    sub_label_list = list((label_list - origin_list) | (label_list))
    # print(sub_label_list)

    new_label_list = []
    for new_label in sub_label_list:
        cursor.execute('select * from %s where label="%s"' % (table_name, new_label))
        new_label = cursor.fetchall()
        new_label = new_label[0]
        # print(new_label)
        new_label_list.append(new_label)
    print(new_label_list)

    cursor.execute('select * from label_type')
    origin_list = []
    origin_result = cursor.fetchall()
    for row in origin_result:
        origin_list.append(row)
    origin_list = list(set(new_label_list) | set(origin_list))
    print(origin_list)

    for tuple_i in origin_list:
        print(tuple_i)
        cursor.execute('insert into label_type1 VALUES("%s", "%s")' % (tuple_i[0], tuple_i[1]))
    # connect.commit()


combine("yanjing_baiwei_label_type")
