from nlpdb import DB
import s3manager
import json

def main():
    list = s3manager._s3_list()
    for key in [x for x in list if '3/' in x]:
        args = s3manager._s3_get_object(key)
        stmt_c = 'insert ignore into nlp_isam.comments_isam values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        stmt_u = 'insert ignore into nlp_isam.users_isam values (%s, %s, %s, %s, %s, %s, %s)'
        if 'users/' in key:
            with DB() as db:
                db.custom_put_many(stmt_u, args)
        elif 'comments/' in key:
            with DB() as db:
                db.custom_put_many(stmt_c, args)
        print('Added {} to Db'.format(key))