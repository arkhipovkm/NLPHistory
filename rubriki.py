from nlpdb import DB
import json
from s3manager import _s3_upload, _s3_make_public, _s3_list
import s3manager
from pprint import pprint

def get_rubrics():
    with open('rubriki_list.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    rubrics = []
    for line in lines:
        line = line.strip('\n')
        line = line.strip('\ufeff')
        if '#' in line:
            group = line.split('#')[-1]
            continue
        r = line.split('. ')[-1].split(', ')
        num = int(line.split('. ')[0])
        '''
        rplus = []
        for item in r:
            if '+' in item or '- ' in item:
                rplus.append('+ '+item)
            else:
                rplus.append(item)
                '''
        rubrics.append({'group': group, 'num': num, 'rubrics': r})
    return rubrics

def get_comments(rubric):
    rubric_comments = []
    for expr in rubric:
        expr_lower = expr.lower()
        expr_upper = expr.upper()
        expr_cap = expr.capitalize()
        stmt = '''select * from nlp_isam.comments_isam_old_text where match(text) against(%s in boolean mode)'''
                                                                        #or
                                                                        #match(text) against(%s in boolean mode) or
                                                                        #match(text) against(%s in boolean mode) or
                                                                        #match(text) against(%s in boolean mode)'''
        args = (expr, expr_lower, expr_upper, expr_cap)
        with DB() as db:
            res = db.custom_get(stmt, args)
            def getmeta(comm_id):
                return db.custom_get('''select user_id, reply_to_user is not null as isreply from comments_isam_old join users_isam on comments_isam_old.user_id=users_isam.id where comments_isam_old.id=%s''', (comm_id, ))[0]

            resmeta = [[x]+list(getmeta(x[0])) for x in res]

        def filter_unique(resmeta):
            seen = set()
            result = []
            for x in resmeta:
                if x[-2] not in seen:
                    result.append(x)
                    seen.add(x[-2])
            return result

        def filter_primary(resmeta):
            return [x for x in resmeta if x[-1] == 0]

        #resmeta = filter_primary(resmeta)
        #rubric_comments += filter_unique(resmeta)
        rubric_comments += filter_unique(resmeta)
        #del res
        print('Acquired comments for rubric: {}'.format(expr))
    return rubric_comments

def main():
    rubrics = get_rubrics()
    #rubrics = [x for x in rubrics if x['num'] in [48]]

    def saveall():
        def save_full(comments):
            res = {'group': rubric['group'], 'num': rubric['num'], 'rubric': rubric['rubrics'], 'count': len(comments), 'comments': comments}
            key = 'rubrics/old/{}.json'.format(rubric['num'])
            _s3_upload(json.dumps(res), key)
            _s3_make_public(key)
            return None
        for rubric in rubrics:
            comments = get_comments(rubric['rubrics'])
            save_full(comments)
    
    def savemeta():
        total = 0
        result= []
        for rubric in rubrics:
            comments = get_comments(rubric)
            res = {'rubric': rubric, 'count': len(comments)}
            result.append(res)
            total += len(comments)
        sorted_result = sorted(result, key=lambda x: x['count'], reversed=True)
        result_dict = {'total': total, 'rubrics': sorted_result}
        key='rubrics/meta/unique_primary.json'
        _s3_upload(json.dumps(result_dict), key)
        _s3_make_public(key)
        return None
        
    def populate_rubrics():
        for n in []:
            key = 'rubrics/old/{}.json'.format(n)
            js = s3manager._s3_get_object(key)
            rubric_num = n
            rubric_name = js['rubric']
            ids = [x[0][0] for x in js['comments']]
            with DB() as db:
                db.custom_put_many('insert ignore into id_rubric (idx, comment_id, rubric_id) values (%s, %s, %s)',
                                   tuple(zip(['{}_{}'.format(n, x) for x in ids],
                                             [x for x in ids],
                                             [n for x in ids])))

    def take_15_300():
        with DB() as db:
            first_15 = db.custom_get('select rubric_id from ')

    saveall()
    #savemeta()
    #populate_rubrics()

    #with open('result_rubrics_comments_dict_full.json', 'w') as f:
    #    json.dump(result_dict, f)

def publicify():
    for n in range(80):
        key = 'rubrics/{}.json'.format(n)
        _s3_make_public(key)
        print(key)

if __name__ == '__main__':
    main()
    #rubs = get_rubrics()
    #pprint(rubs)