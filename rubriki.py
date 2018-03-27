from nlpdb import DB
import json
from s3manager import _s3_upload, _s3_make_public, _s3_list
import s3manager

def get_rubrics():
    with open('rubriki_list.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    rubrics = []
    for line in lines[1:]:
        r = line.strip('\n').split('. ')[-1].split(', ')
        rubrics.append(r)
    return rubrics

def get_comments(rubric):
    rubric_comments = []
    for expr in rubric:
        stmt = '''select * from nlp_isam.comments_isam_young_text where match(text) against(%s in boolean mode)'''
        args = (expr, )
        with DB() as db:
            res = db.custom_get(stmt, args)
            def getmeta(comm_id):
                return db.custom_get('''select user_id, reply_to_user is not null as isreply from comments_isam_young join users_isam on comments_isam_young.user_id=users_isam.id where comments_isam_young.id=%s''', (comm_id, ))[0]

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

        resmeta = filter_primary(resmeta)
        rubric_comments += filter_unique(resmeta)
        print('Acquired comments for rubric: {}'.format(expr))
    return rubric_comments

def main():
    rubrics = get_rubrics()

    def saveall():
        def save_full(comments):
            res = {'rubric': rubric, 'count': len(comments), 'comments': comments}
            key = 'rubrics/unique_primary/{}.json'.format(rubrics.index(rubric))
            _s3_upload(json.dumps(res), key)
            _s3_make_public(key)
            return None
        for rubric in rubrics:
            comments = get_comments(rubric)
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
        for n in range(78):
            key = 'rubrics/unique_primary/{}.json'.format(n)
            js = s3manager._s3_get_object(key)
            rubric_num = n
            rubric_name = js['rubric']
            ids = [x[0][0] for x in js['comments']]
            with DB() as db:
                db.custom_put_many('insert into id_rubric (id, rubric_id) values (%s, %s)', tuple(zip([x for x in ids],
                                                                                                      [n for x in ids])))

    #saveall()
    #savemeta()
    populate_rubrics()

    #with open('result_rubrics_comments_dict_full.json', 'w') as f:
    #    json.dump(result_dict, f)

def publicify():
    for n in range(80):
        key = 'rubrics/{}.json'.format(n)
        _s3_make_public(key)
        print(key)

if __name__ == '__main__':
    main()