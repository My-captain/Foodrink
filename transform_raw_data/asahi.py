# -*- coding:utf-8 -*-
import pymysql


from_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='tbs_data', charset='utf8')
from_cursor = from_connect.cursor()

to_connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8')
to_cursor = to_connect.cursor()

extract_sql = 'select * from %s'
insert_sql = 'insert into %s values("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", ' \
             '"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")'


def transform_raw_data(from_table_name, to_table_name):
    from_cursor.execute(extract_sql % from_table_name)
    raw_data = from_cursor.fetchall()

    from_cursor.execute(extract_sql % from_table_name+"label")
    label_raw_data = from_cursor.fetchall()

    from_cursor.execute(extract_sql % from_table_name+"label_image")
    image_label_raw_data = from_cursor.fetchall()
    print(raw_data)
    # print(label_raw_data)
    # print(image_label_raw_data)
    for row in raw_data:
        image_tags = row[19].split("]")[1].strip("'").split("''")
        text_labels = row[4].strip("[").strip("]").strip("'").split("','")
        # print(text_labels)
        # print(image_tags, text_labels)
        image_who = []
        image_when = []
        image_what = []
        image_how = []
        image_where = []
        image_others = []
        text_who = []
        text_when = []
        text_what = []
        text_how = []
        text_where = []
        text_others = []
        for image_tag in image_tags:
            for image_label_row in image_label_raw_data:
                if image_label_row[3]:
                    if image_label_row[0] == image_tag:
                        label_type = image_label_row[3]
                        if label_type == 1:
                            image_where.append(image_tag)
                            break
                        elif label_type == 2:
                            image_when.append(image_tag)
                            break
                        elif label_type == 4:
                            image_who.append(image_tag)
                            break
                        elif label_type == 5:
                            image_what.append(image_tag)
                            break
                        elif label_type == 6:
                            image_how.append(image_tag)
                            break

        for text_tag in text_labels:
            for text_label_row in label_raw_data:
                if text_label_row[2]:
                    if text_label_row[0] == text_tag:
                        label_type = text_label_row[2]
                        if label_type == 1:
                            text_where.append(text_tag)
                            break
                        elif label_type == 2:
                            text_when.append(text_tag)
                            break
                        elif label_type == 3:
                            text_how.append(text_tag)
                            break
                        elif label_type == 5:
                            text_what.append(text_tag)
                            break
                        elif label_type == 9:
                            text_who.append(text_tag)
                            break
        image_who = list(set(image_who))
        image_when = list(set(image_when))
        image_what = list(set(image_what))
        image_how = list(set(image_how))
        image_where = list(set(image_where))
        image_others = list(set(image_tags)-set(image_who)-set(image_when)-set(image_what)-set(image_how)-set(image_where))
        text_who = list(set(text_who))
        text_when = list(set(text_when))
        text_what = list(set(text_what))
        text_how = list(set(text_how))
        text_where = list(set(text_where))
        text_others = list(set(text_labels) - set(text_who) - set(text_when) - set(text_what) - set(text_how) - set(text_where))
        # print(insert_sql % (to_table_name, row[0], row[1], row[2], row[4], row[5], row[6], row[7], row[8],
        #                                 row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],
        #                                 row[18], image_tags, text_who, text_when, text_what, text_how, text_where,
        #                                 image_who, image_when, image_what, image_how, image_where))
        to_cursor.execute(insert_sql % (to_table_name, row[0], row[1], row[2], row[4], row[5], row[6], row[7], row[8],
                                        row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],
                                        row[18], image_tags, text_who, text_when, text_what, text_how, text_where, text_others,
                                        image_who, image_when, image_what, image_how, image_where, image_others))

    to_connect.commit()


transform_raw_data("asahi", "asahi_raw_data")
