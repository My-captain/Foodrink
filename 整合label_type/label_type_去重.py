# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/29 下午4:20
# @Author  : Mr.Robot
# @File    : label_type_去重.py
# ==========================================

import pymysql

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8mb4')
cursor = connect.cursor()

cursor.execute('select * from label_type1')
result = cursor.fetchall()

label_type_list = {}
for row in result:
    if row[0] in label_type_list.keys():
        if label_type_list[row[0]] != "None":
            continue
        else:
            label_type_list[row[0]] = row[1]
            continue
    else:
        label_type_list[row[0]] = row[1]

for key in label_type_list.keys():
    cursor.execute('insert into label_type2 VALUES("%s", "%s")' % (key, label_type_list[key]))

connect.commit()
