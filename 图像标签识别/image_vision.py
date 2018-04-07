# -*- coding: utf-8 -*-
# ==========================================
# @Time    : 2018/3/30 下午2:30
# @Author  : Mr.Robot
# @File    : image_vision.py
# ==========================================

import io
import os
import pymysql

connect = pymysql.connect(host='localhost', port=3306, user='root', password='echo', db='Foodrink', charset='utf8mb4')
cursor = connect.cursor()



from google.cloud import vision
from google.cloud.vision import types

client = vision.ImageAnnotatorClient()

path = "/Users/mr.robot/Desktop/Foodrink_pic"  # 文件夹目录
files = os.listdir(path)  # 得到文件夹下的所有文件名称

index = 0
for i in files:
    index += 1
    file_name = i
    with io.open(path+"/"+file_name, 'rb') as image_file:
        content = image_file.read()

    image = types.Image(content=content)

    # Performs label detection on the image file
    response = client.label_detection(image=image)
    labels = response.label_annotations

    label_list = []
    for label in labels:
        label_list.append(label.description)
    print("当前第 %s 张图片识别结果%s：" % (index, str(label_list).replace('"', "'")))
    print('insert into image_labels VALUES("%s", "%s")' % (file_name, str(label_list).replace('"', "'")))
    cursor.execute('insert into image_labels VALUES("%s", "%s")' % (file_name, str(label_list).replace('"', "'")))
    connect.commit()
