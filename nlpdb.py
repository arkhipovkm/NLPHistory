from platform import system
from time import sleep
from datetime import datetime
import json
import os
import random
import string
import s3manager
import json

#HOST = '37.139.47.125'
HOST = 'nlpdbinstance.c5zyxzo5smzm.us-west-2.rds.amazonaws.com'
USER = 'kirill'
PASS = 'kmsr890714'

class DB():

    class DBDecorator(object):
        @staticmethod
        def put(func):
            def inner(self, *args, **kwargs):
                stmt, args = func(self, *args, **kwargs)
                retries = 0
                while retries < 5:
                    try:
                        self.cur.execute(stmt, args)
                        self.con.commit()
                        break
                    except Exception as e:
                        print('Exception in DB: iterating.. Error: ' + str(e))
                        self.close()
                        self.connect()
                        sleep(1)
                        retries += 1
                        continue
            return inner

        @staticmethod
        def put_many(func):
            def inner(self, *args, **kwargs):
                stmt, args = func(self, *args, **kwargs)
                retries = 0
                while retries < 5:
                    try:
                        self.cur.executemany(stmt, args)
                        self.con.commit()
                        break
                    except Exception as e:
                        print('Exception in DB: iterating.. Error: ' + str(e))
                        self.close()
                        self.connect()
                        sleep(1)
                        retries += 1
                        continue
            return inner

        @staticmethod
        def get(func):
            def inner(self, *args, **kwargs):
                stmt, args = func(self, *args, **kwargs)
                retries = 0
                while retries < 5:
                    try:
                        self.cur.execute(stmt, args)
                        res = self.cur.fetchall()
                        self.con.commit()
                        return res
                    except Exception as e:
                        print('Exception in DB: iterating.. Error: ' + str(e))
                        sleep(1)
                        self.close()
                        self.connect()
                        retries += 1
                        continue
            return inner


    def __init__(self):
        self.connect()

    def connect(self):
        '''
        if system() == 'Windows':
            import pymysql
            
            con = pymysql.connect(user='marianne',
                                  database = 'nlp',
                                  host = HOST,
                                  password=PASS,
                                  charset='utf8mb4')
                                  
            con = pymysql.connect(user=USER,
                                  database='nlp_isam',
                                  host=HOST,
                                  password=PASS,
                                  charset='utf8mb4')
                                  '''
        #else:
        import mysql.connector
        con = mysql.connector.connect(user=USER,
                                        database = 'nlp_isam',
                                        host = HOST,
                                        password=PASS,
                                        charset='utf8mb4',
                                        collation='utf8mb4_unicode_ci')

        con.get_warnings=False

        self.con = con
        self.cur = self.con.cursor(named_tuple=True)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if 'Windows' not in system():
            self.close()

    def close(self):
        self.con.close()

    @DBDecorator.get
    def custom_get(self, stmt, args):
        return stmt, args

    @DBDecorator.put
    def custom_put(self, stmt, args):
        return stmt, args

    @DBDecorator.put_many
    def custom_put_many(self, stmt, args):
        return stmt, args


    @DBDecorator.put_many
    def add_users(self, users):
        stmt = "INSERT IGNORE INTO users VALUES (%s, %s, %s, %s, %s, %s, %s)"

        args = tuple(zip([x['id'] for x in users],
                         [x['first_name'] for x in users],
                         [x['last_name'] for x in users],
                         [x['sex'] if 'sex' in x.keys() else None for x in users],
                         [x['bdate'] if 'bdate' in x.keys() else None for x in users],
                         [x['country']['id'] if 'country' in x.keys() else None for x in users],
                         [x['city']['id'] if 'city' in x.keys() else None for x in users]))
        return stmt, args

    @staticmethod
    def add_users_json(users):
        stmt = "INSERT IGNORE INTO users VALUES (%s, %s, %s, %s, %s, %s, %s)"

        args = tuple(zip([x['id'] for x in users],
                         [x['first_name'] for x in users],
                         [x['last_name'] for x in users],
                         [x['sex'] if 'sex' in x.keys() else None for x in users],
                         [x['bdate'] if 'bdate' in x.keys() else None for x in users],
                         [x['country']['id'] if 'country' in x.keys() else None for x in users],
                         [x['city']['id'] if 'city' in x.keys() else None for x in users]))
        r = ''.join([random.choice(string.ascii_uppercase+string.ascii_lowercase) for x in range(12)])
        key = '3/users/Users_args_{}'.format(r)
        #from io import StringIO
        #with StringIO() as f:
        #with open(filename, 'w') as f:
        js = json.dumps(args)
        s3manager._s3_upload(js, key)

    @DBDecorator.put_many
    def add_comments(self, group, post, comments):
        stmt = "INSERT IGNORE INTO comments2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        args = tuple(zip(['{}_{}_{}'.format(group, post, x['id']) for x in comments],
                         [group for x in range(len(comments))],
                         [post for x in range(len(comments))],
                         [x['id'] for x in comments],
                         [x['from_id'] for x in comments],
                         [x['text'] for x in comments],
                         [x['likes']['count'] for x in comments],
                         [datetime.fromtimestamp(x['date']).strftime('%Y-%m-%d %H:%M:%S') for x in comments],
                         [x['reply_to_user'] if 'reply_to_user' in x.keys() else None for x in comments],
                         [x['reply_to_comment']  if 'reply_to_comment' in x.keys() else None for x in comments]))
        return stmt, args

    @staticmethod
    def add_comments_json(group, post, comments):
        stmt = "INSERT IGNORE INTO comments2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        args = tuple(zip(['{}_{}_{}'.format(group, post, x['id']) for x in comments],
                         [group for x in range(len(comments))],
                         [post for x in range(len(comments))],
                         [x['id'] for x in comments],
                         [x['from_id'] for x in comments],
                         [x['text'] for x in comments],
                         [x['likes']['count'] for x in comments],
                         [datetime.fromtimestamp(x['date']).strftime('%Y-%m-%d %H:%M:%S') for x in comments],
                         [x['reply_to_user'] if 'reply_to_user' in x.keys() else None for x in comments],
                         [x['reply_to_comment']  if 'reply_to_comment' in x.keys() else None for x in comments]))
        r = ''.join([random.choice(string.ascii_uppercase+string.ascii_lowercase) for x in range(3)])
        key = '3/comments/Comments_args_g{}_p{}_c{}_r{}'.format(group, post, len(comments), r)
        #from io import StringIO
        #with StringIO() as f:
        #with open(filename, 'w') as f:
        js = json.dumps(args)
        s3manager._s3_upload(js, key)
            

    def add_done(self, group, post, comments_count, processed_count):
        self._add_done_ins(group, post, comments_count, processed_count)
        self._add_done_upd(group, post, comments_count, processed_count)

    @DBDecorator.put
    def _add_done_ins(self, group, post, comments_count, processed_count):
        stmt = "INSERT IGNORE INTO done VALUES (%s, %s, %s, %s, %s)"
        args = ('{}_{}'.format(group, post),
                group,
                post,
                comments_count,
                processed_count)
        return stmt, args

    @DBDecorator.put
    def _add_done_upd(self, group, post, comments_count, processed_count):
        stmt = "UPDATE done SET processed_count=processed_count+%s WHERE id=%s"
        args = (processed_count, '{}_{}'.format(group, post))
        return stmt, args

    @DBDecorator.get
    def get_done(self):
        stmt = "SELECT id FROM done WHERE comments_count=processed_count"
        args = ()
        return stmt, args

    @DBDecorator.get
    def get_done_by_group(self, group):
        stmt = "SELECT count(distinct post_id) FROM done WHERE group_id=%s AND processed_count >= comments_count"
        args = (group, )
        return stmt, args

    @DBDecorator.put
    def put_next_offset(self, offset, group):
        stmt = 'UPDATE last_offsets SET offset=%s WHERE last_offsets.group=%s'
        args = (offset, group)
        return stmt, args

    @DBDecorator.get
    def get_next_offset(self, group):
        stmt = 'SELECT offset from last_offsets WHERE last_offsets.group=%s'
        args = (group, )
        return stmt, args