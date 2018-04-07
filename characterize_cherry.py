from cherrypy import expose, response, request, quickstart, config, popargs, tree
import json
from nlpdb import DB
import pandas as pd
import numpy as np
import os

#@popargs('rubric_id')
class Characterize():
    '''
    @expose
    def index(self, rubric_id=0):
        response.headers['Content-Type'] = 'text/html'
        response.headers['Access-Control-Allow-Origin'] = '*'

        with open('characterization.html', 'rb') as f:
            return f
            '''

    @expose
    def process_xar(self, **kwargs):
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Origin'] = '*'

        index = kwargs['index']
        xar = kwargs['xar']
        xar_list = kwargs['xar_list']
        note = kwargs['note']
        print(kwargs)
        rubric_id = kwargs['rubric_id']
        chefdoeuvre = kwargs['chefdoeuvre']

        xar = xar_list if xar_list else xar

        idx = '{}_{}'.format(rubric_id, index)

        with DB() as db:
            db.custom_put('update id_rubric set characteristic=%s, note=%s, is_chefdoeuvre=%s where idx=%s', (xar, note, chefdoeuvre, idx))
            total = db.custom_get('select count(*) from id_rubric where rubric_id=%s and is_deleted=0', (rubric_id, ))[0][0]
            done = db.custom_get('select count(*) from id_rubric where rubric_id=%s and characteristic is not null', (rubric_id, ))[0][0]
            xars = db.custom_get('select distinct characteristic from id_rubric where rubric_id=%s', (rubric_id, ))

            xars = [x[0] for x in xars if x[0]]

        resp = {'done': done,
                'total': total,
                'xars': xars}

        return json.dumps(resp).encode('utf-8')

    @expose
    def delete_idx(self, **kwargs):
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Origin'] = '*'

        index = kwargs['index']
        rubric_id = kwargs['rubric_id']

        idx = '{}_{}'.format(rubric_id, index)
        with DB() as db:
            db.custom_put('update id_rubric set is_deleted=1 where idx=%s', (idx, ))
            total = db.custom_get('select count(*) from id_rubric where rubric_id=%s and is_deleted=0', (rubric_id, ))[0][0]
            done = db.custom_get('select count(*) from id_rubric where rubric_id=%s and characteristic is not null', (rubric_id, ))[0][0]

        resp = {'done': done,
                'total': total}

        return json.dumps(resp).encode('utf-8') 

    @expose
    def get_rubric(self, rubric_id=None):
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Origin'] = '*'
        rubric_id = int(rubric_id)
        ddf = df[df.rubric_id==rubric_id].sort_values(by=['likes'], ascending=False).drop_duplicates()
        #ddf = df[df.rubric_id==rubric_id].drop_duplicates()
        #print(len(ddf))
        ddf['age'] = (ddf.date - ddf.bdate).dt.days/365
        group_name = ddf.group.unique()[0]
        max_likes = int(max(ddf.likes))
        mean_likes = int(np.mean(ddf.likes))

        total_raw = len(ddf)

        with DB() as db:
            deleted = db.custom_get('select comment_id from id_rubric where rubric_id=%s and is_deleted=1', (rubric_id, ))
            deleted = [x[0] for x in deleted]
            done = db.custom_get('select comment_id from id_rubric where rubric_id=%s and characteristic is not null', (rubric_id, ))
            done = [x[0] for x in done]

            xars = db.custom_get('select distinct characteristic from id_rubric where rubric_id=%s', (rubric_id, ))

            xars = [x[0] for x in xars if x[0]]

        ddf = ddf.drop(pd.Series(deleted), axis=0)
        ddf = ddf.drop(pd.Series(done), axis=0)

        ddf = ddf.reset_index()

        ddf.date = ddf.date.dt.strftime('%Y-%m-%d')
        ddf = ddf.drop('bdate', axis=1)

        rubric_name = pd.read_pickle('rubric_names.pickle')[rubric_id]
        #group_name = ddf.group.unique()[0]
        #ddf_js = ddf.head().to_json(orient='values')

        obj = {'rubric_id': rubric_id,
               'rubric_name': rubric_name,
               'group_name': group_name,
               'done_count': len(done),
               'total_count': len(ddf),
               'likes_max': max_likes,
               'likes_mean': mean_likes,
               'xars_list': xars,
               'whole_data': ddf.to_dict('records')[:300]}

        return json.dumps(obj).encode('utf-8')

if __name__ == '__main__':
    df = pd.read_pickle('df_indexed.pickle')
    #print(len(df))
    config.update({'tools.staticdir.index': "index.html",
                   'tools.staticdir.dir': os.getcwd(),
                   'tools.staticdir.on': True,
                   'server.socket_host': '0.0.0.0'})
    quickstart(Characterize())