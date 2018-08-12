#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3 as sq
import csv
import gzip


# create table文を実行する
# テーブルがなければ新規作成する
def create_table(database, table_name, key_list):
    # sqlの作成
    key1 = ''
    for k, v in key_list.items():
        key1 = key1 + k + ' ' + v + ', '
    else:
        key1 = '(' + key1.strip(', ') + ')'
        sql = 'create table if not exists ' + table_name + ' ' + key1
    print('------------------------------------------------')
    print(sql)

    # sql実行
    try:
        database.execute(sql)
        print('テーブルを作成しました：' + table_name)
    except:
        print('エラーが発生したため、テーブル作成をスキップしました:' + table_name)
        print('------------------------------------------------')
    finally:
        database.commit()
        print('------------------------------------------------')


# Insert文を実行する
# エラーが発生した場合は処理をスキップする
def insert_data(database, file_name, table_name, key_list, delimiter_str=','):
    # sqlの作成
    key1 = ''
    key2 = ''
    for key in key_list:
        key1 = key1 + key + ', '
        key2 = key2 + ':' + key + ', '
    else:
        key1 = '(' + key1.strip(', ') + ')'
        key2 = '(' + key2.strip(', ') + ')'
        sql = 'insert into ' + table_name + ' ' + key1 + ' values ' + key2
        sql = sql.replace('TABLE', table_name)

    # 読み込むファイルの拡張子別に処理を行う
    extension = file_name.split('.')
    extension = extension[len(extension) - 1]
    if extension == 'gz':
        f = gzip.open(file_name, 'rt', encoding='UTF-8')
    else:
        f = open(file_name, 'r', encoding='UTF-8')

    # delimiter_strでファイルを分割して読み込む
    reader = csv.reader(f, delimiter=delimiter_str)
    print('------------------------------------------------')
    print(sql)
    # 1行ずつ処理
    for row in reader:
        # 読み込んだデータを辞書化する
        d = dict(zip(key_list, row))
        # sql実行
        try:
            database.execute(sql, d)
        except:
            print('エラーが発生したためスキップしました:' + str(row))
    else:
        database.commit()
        f.close()
        print('データをInsertしました')
        print('------------------------------------------------')


# select文を実行する
def select_data(cursor, sql):
    print('------------------------------------------------')
    print(sql)
    for row in cursor.execute(sql):
        print(row)
    else:
        print('------------------------------------------------')


# メイン処理
# dbオブジェクト作成
db = sq.connect('./test.db')
c = db.cursor()

# usersテーブル作成・読み込み
create_keys = {
    'userId': 'integer primary key',
    'name': 'text',
    'age': 'integer',
    'gender': 'text'
}
create_table(db, 'users', create_keys)
insert_keys = (
    'userId',
    'name',
    'age',
    'gender'
)
insert_data(db, 'users.log', 'users', insert_keys)

# RentalListテーブル作成・読み込み
create_keys = {
    'id': 'integer primary key',
    'userId': 'integer',
    'returnDate': 'text',
    'title': 'text',
    'author': 'text'
}
create_table(db, 'RentalList', create_keys)
insert_keys = (
    'userId',
    'returnDate',
    'title',
    'author'
)
insert_data(db, 'RentalList.csv.gz', 'RentalList', insert_keys, '\t')

# 各種select文実行
select_where_sql = 'select * from users where gender = "male"'
select_data(c, select_where_sql)

select_where_sql = 'select * from users where age >= 30'
select_data(c, select_where_sql)

select_where_sql = 'select * from users inner join RentalList on users.userId = RentalList.userId'
select_data(c, select_where_sql)

select_where_sql = 'select * from users left outer join RentalList on users.userId = RentalList.userId'
select_data(c, select_where_sql)

# 最後にdbを閉じる
db.close()
