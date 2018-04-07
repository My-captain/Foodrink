# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/29 下午5:09
# @Author  : Mr.Robot
# @File    : spider_images.py
# ==========================================

import requests
import pymysql
import os


connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink',
                          charset='utf8mb4')
cursor = connect.cursor()


def spider_image(table_name):
    cursor.execute('select urls from %s' % table_name)
    result = cursor.fetchall()
    # print(result)
    for row in result:
        urls = row[0]
        urls = urls.strip("[").strip("]").strip("'")
        image_url_list = urls.split("', '")
        if image_url_list[0] != "":
            for image_url in image_url_list:
                image_url = "http:" + image_url
                image_name = image_url.split("/")[len(image_url.split("/")) - 1]
                print(image_name)
                if os.path.exists(r'/Users/mr.robot/Desktop/Foodrink_pic/' + image_name):
                    continue
                with open(r'/Users/mr.robot/Desktop/Foodrink_pic/' + image_name, 'xb') as fd:
                    r = requests.get(image_url, stream=True)
                    for chunk in r.iter_content():
                        fd.write(chunk)


spider_image("yanjing_raw_data")
spider_image("baiwei_raw_data")
