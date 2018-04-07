# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/30 下午4:37
# @Author  : Mr.Robot
# @File    : classifier_image_type.py
# ==========================================

import pymysql

image_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='tbs_data', charset='utf8')
image_cursor = image_connect.cursor()

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8')
cursor = connect.cursor()


def classifier_image_type(brand_name):
    type = False
    cursor.execute('select id,tags from %s' % (brand_name + "_raw_data"))
    result = cursor.fetchall()
    for row in result:
        id = row[0]
        tags = row[1]
        if tags == "[]":
            continue
        tag_list = tags.strip("[").strip("]").strip("'").split("', '")
        # print(set(tag_list))
        img_what = []
        img_when = []
        img_where = []
        img_who = []
        img_how = []
        for tag in tag_list:
            image_cursor.execute('select type from pandalabel_image where label="%s"' % tag)
            result = image_cursor.fetchall()
            if len(result) > 0 and result[0][0] != None:
                type = result[0][0]
            else:
                # print("yes")
                image_cursor.execute('select type from qingdaolabel_image where label="%s"' % tag)
                result = image_cursor.fetchall()
                if len(result) > 0 and result[0][0] != None:
                    type = result[0][0]
                else:
                    image_cursor.execute('select type from kylinlabel_image where label="%s"' % tag)
                    result = image_cursor.fetchall()
                    if len(result) > 0 and result[0][0] != None:
                        type = result[0][0]
                    else:
                        image_cursor.execute('select type from snowlabel_image where label="%s"' % tag)
                        result = image_cursor.fetchall()
                        if len(result) > 0 and result[0][0] != None:
                            type = result[0][0]
                        else:
                            # print("yes")
                            image_cursor.execute('select type from asahilabel_image where label="%s"' % tag)
                            result = image_cursor.fetchall()
                            if len(result) > 0 and result[0][0] != None:
                                type = result[0][0]
            if type == 1:
                img_where.append(tag)
                break
            elif type == 2:
                img_when.append(tag)
                break
            elif type == 4:
                img_who.append(tag)
                break
            elif type == 5:
                img_what.append(tag)
                break
            elif type == 6:
                img_how.append(tag)
                break
            type = False
        # print(set(img_how))
        img_others = set(tag_list)-set(img_how)-set(img_what)-set(img_when)-set(img_where)-set(img_who)
        img_others = str(list(img_others)).replace('"', "'")
        # print(img_how, img_what, img_when, img_where, img_who)
        # print(img_others)
        print('update %s set img_who="%s",img_when="%s",img_what="%s",img_how="%s",img_where="%s'
                       '",img_others="%s" where id="%s"' % (brand_name+"_raw_data", img_who, img_when, img_what,
                                                            img_how, img_where, img_others, id))
        cursor.execute('update %s set img_who="%s",img_when="%s",img_what="%s",img_how="%s",img_where="%s'
                       '",img_others="%s" where id="%s"' % (brand_name+"_raw_data", img_who, img_when, img_what,
                                                            img_how, img_where, img_others, id))


classifier_image_type("yanjing")
classifier_image_type("baiwei")
connect.commit()
