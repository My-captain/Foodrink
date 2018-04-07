# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/29 下午1:32
# @Author  : Mr.Robot
# @File    : json_to_db.py
# ==========================================

import pymysql
import json
import re

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink',
                          charset='utf8mb4')
cursor = connect.cursor()
json_to_mysql_sql = 'insert into %s VALUES("%s", "%s", "%s", NULL, NULL, "%s", "%s", NULL, "%s", "%s", ' \
                    '"%s", "%s", "%s", "%s", "%s", "%s", NULL, "%s", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ' \
                    'NULL, NULL, NULL, NULL, NULL)'


def filter_emoji(text):
    highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    return highpoints.sub(u'', text)


def json_to_mysql(file_namei, table_name):
    file_name = r"/Users/mr.robot/Downloads/weibo-" + file_namei + r".json"
    with open(file_name, encoding="utf8", mode="r") as load_json_file:
        load_dict = json.load(load_json_file)
        print(len(load_dict))
        for index, each_json in enumerate(load_dict):
            # keyword_from_search = each_json["keyword_from_search"]
            user_name = each_json["user_name"]
            content = each_json["content"].replace('"', "'")
            content = filter_emoji(content)
            url = each_json["url"]
            time = each_json["time"]
            topics = each_json["topics"]
            comment = each_json["comment"]
            facilities = each_json["from"]
            facilities = filter_emoji(facilities)
            place = each_json["place"]
            emojis = each_json["emojis"]
            pics = each_json["pics"]
            like = each_json["like"]
            forward = each_json["forward"]
            favorite = each_json["favorite"]
            # urls = each_json["urls"]
            id = 10000+index
            if file_namei == "百威啤酒":
                id += 907
            id = str(id)
            print(json_to_mysql_sql % (table_name, id, user_name, content, topics, emojis, place, url, time,
                                       facilities, favorite, forward, comment, like, pics))
            cursor.execute(json_to_mysql_sql % (table_name, id, user_name, content, topics, emojis, place, url, time,
                                                facilities, favorite, forward, comment, like, pics))


json_to_mysql("燕京啤酒", "yanjing_raw_data")
json_to_mysql("百威啤酒", "baiwei_raw_data")

connect.commit()
