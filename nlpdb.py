from platform import system
from time import sleep
from datetime import datetime

HOST = '37.139.47.125'
PASS = 'Mmsr960514spvk'

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
                    except Excepion as e:
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
        if system() == 'Windows':
            import pymysql
            con = pymysql.connect(user='marianne',
                                  database = 'nlp',
                                  host = HOST,
                                  password=PASS,
                                  charset='utf8mb4')
        else:
            import mysql.connector
            con = mysql.connector.connect(user='marianne',
                                            database = 'nlp',
                                            host = 'localhost',
                                            password=PASS,
                                            charset='utf8mb4',
                                            collation='utf8mb4_unicode_ci')

        con.get_warnings=False

        self.con = con
        self.cur = self.con.cursor()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if 'Windows' not in system():
            self.close()

    def close(self):
        self.con.close()

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


    @DBDecorator.put_many
    def add_comments(self, group, post, comments):
        stmt = "INSERT IGNORE INTO comments VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

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
        stmt = "SELECT count(id) FROM done WHERE group_id=%s AND processed_count >= comments_count"
        args = (group, )
        return stmt, args