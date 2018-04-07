# -*- coding:utf-8 -*-
import pymysql


from_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='tbs_data', charset='utf8')
from_cursor = from_connect.cursor()

to_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8')
to_cursor = to_connect.cursor()

extract_sql = 'select * from %s'
insert_sql = 'insert into %s values("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", ' \
             '"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")'


def classfier_table(table_name):
    to_cursor.execute(extract_sql % table_name)
    raw_data = to_cursor.fetchall()
    for row in raw_data:
        txt_who = []
        txt_when = []
        txt_what = []
        txt_how = []
        txt_where = []
        txt_others = []
        label_list = row[3].strip("[").strip("]").strip("'").split("', '")
        for label_i in label_list:
            to_cursor.execute('select * from label_type2 where label="%s"' % label_i)
            label_type = to_cursor.fetchall()
            print(label_type)
            label_type = list(label_type)
            label_type = label_type[0]
            label_type = list(label_type)
            print(label_type)
            if label_type[1] == '1':
                txt_who.append(label_i)
            elif label_type[1] == '2':
                txt_when.append(label_i)
            elif label_type[1] == '3':
                txt_what.append(label_i)
            elif label_type[1] == '4':
                txt_how.append(label_i)
            elif label_type[1] == '5':
                txt_where.append(label_i)
        txt_who = list(set(txt_who))
        txt_when = list(set(txt_when))
        txt_what = list(set(txt_what))
        txt_how = list(set(txt_how))
        txt_where = list(set(txt_where))
        txt_others = list(set(label_list)-set(txt_where)-set(txt_who)-set(txt_when)-set(txt_what)-set(txt_how))
        print(txt_how, txt_what, txt_when, txt_who, txt_others, txt_where)
        print('update %s set txt_who="%s" txt_when="%s" txt_what="%s" txt_how="%s" txt_where="%s" '
                          'txt_others="%s" where id="%s"' % (table_name, txt_who, txt_when, txt_what, txt_how, txt_where, txt_others, row[0]))
        to_cursor.execute('update %s set txt_who="%s",txt_when="%s",txt_what="%s",txt_how="%s",txt_where="%s",'
                          'txt_others="%s" where id="%s"' % (table_name, txt_who, txt_when, txt_what, txt_how, txt_where, txt_others, row[0]))
    to_connect.commit()


# classfier_table("kylin_raw_data")
# classfier_table("panda_raw_data")
# classfier_table("qingdao_raw_data")
# classfier_table("snow_raw_data")
classfier_table("yanjing_raw_data")
classfier_table("baiwei_raw_data")
