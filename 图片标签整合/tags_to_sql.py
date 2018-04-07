# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/30 下午3:26
# @Author  : Mr.Robot
# @File    : tags_to_sql.py
# ==========================================

import pymysql

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink',
                          charset='utf8mb4')
cursor = connect.cursor()


def tags_to_sql(brand_name, tags_table):
    cursor.execute('select id,urls from %s' % (brand_name + "_raw_data"))
    raw_data_result = cursor.fetchall()
    for row in raw_data_result:
        id = row[0]
        urls = row[1]
        url_list = urls.strip("[").strip("]").strip("'").split("', '")
        image_name_list = []
        for url in url_list:
            image_name_list.append(url.split("/")[len(url.split("/")) - 1])
        print(image_name_list)

        image_tags = []
        if image_name_list[0] != '':
            for image_name in image_name_list:
                cursor.execute('select labels from %s where image_name="%s"' % (tags_table, image_name))
                result = cursor.fetchall()
                for row in result:
                    row = row[0].strip("[").strip("]").strip("'").split("', '")
                    for tag in row:
                        image_tags.append(tag)

        cursor.execute('update %s set tags="%s" where id="%s"' % (brand_name + '_raw_data', str(image_tags).replace('"', "'"), id))


tags_to_sql("yanjing", "image_labels")
tags_to_sql("baiwei", "image_labels")
connect.commit()
