import os
os.chdir('F:\\Users\\Kirill\\source\\repos\\NLPHistory')

from nlpdb import DB
import requests
import json
from re import sub


KEY1 = '27e35dcab5fb4f7a82ca13a7fbfe8545'
KEY2 = 'ca7cb40b608c4ddba38cb22d0ec394bb'
URL = 'https://westcentralus.api.cognitive.microsoft.com/text/analytics/v2.0/{}'

header = {'Ocp-Apim-Subscription-Key': KEY2,
          'Content-Type': 'application/json',
          'Accept': 'application/json'}


def get_comments():
    with DB() as db:
        comments = db.custom_get('select users.id, stalin.id, stalin.text from users join stalin on users.id=stalin.user_id', ( ))
    return comments


def main(mode='keyPhrases'):
    comments = get_comments('сталин')

    seen = set()
    comms = []
    for comm in comments:
        if comm[0] not in seen:
            comms.append((comm[1], comm[2]))
            seen.add(comm[0])


    result = {}
    for n_sentence in ['1', '2', 'full']:

        comm_list = []
        req_list = []
        for comm in comms:
            comm_dict = dict(zip(['language', 'id', 'text'],
                                 ['ru', comm[0], '.'.join(sub('\[id[0-9]*?\|.*?\]', '', comm[1]).split('.')[:int(n_sentence)]) if n_sentence != 'full' else sub('\[id[0-9]*?\|.*?\]', '', comm[1])]))
            if len(str(comm_list)) + len(str(comm_dict)) < 524288 - 20 and len(comm_list) < 1000:
                comm_list.append(comm_dict)
            else:
                req_list.append(json.dumps({'documents': comm_list}))
                comm_list = []
                continue

        req_list.append(json.dumps({'documents': comm_list}))

        resp_list = []
        for req in req_list:
            resp_list.append(requests.post(URL.format(mode), data=req, headers=header).json())

        result[n_sentence] = resp_list

    with DB() as db:
        for key, resp_list in result.items():
            col = 'score_{}'.format(key) if 'sentiment' in mode else 'keywords_{}'.format(key)
            for resp in resp_list:
                db.custom_put_many('update scores set {}=%s where comment_id=%s'.format(col),
                                   tuple(zip([x['score'] if 'sentiment' in mode else '; '.join(x['keyPhrases']) for x in resp['documents']],
                                             [x['id'] for x in resp['documents']])))


def main_isp():
    KEY = '63e75cbccf7a1cbd88258c5217ca59331310d2f3'
    URL = 'http://api.ispras.ru/texterra/v1/nlp?'
    url = URL + 'targetType=polarity&tweet=true&apikey={}'.format(KEY)

    comments = get_comments()
    mode = 'polarity'
    seen = set()
    comms = []
    for comm in comments:
        if comm[0] not in seen:
            comms.append((comm[1], sub('\[id[0-9]*?\|.*?\]', '', comm[2])))
            seen.add(comm[0])


    result = {}
    for n_sentence in ['full']:

        comm_list = []
        req_list = []
        for comm in comms:
            comm_dict = {'text': comm[1], 'id': comm[0]}
            if len(str(comm_list)) + len(str(comm_dict)) < 524288 - 20 and len(comm_list) < 50:
                comm_list.append(comm_dict)
            else:
                req_list.append(json.dumps(comm_list))
                comm_list = []
                continue

        req_list.append(json.dumps(comm_list))

        resp_list = []
        for req in req_list:
            _req = json.loads(req)
            texts = [x['text'] for x in _req]
            resp = requests.post(url, data=req, headers={'Content-Type': 'application/json', 'Accept': 'application/json'}).json()
            r = [{'annotations': x['annotations'], 'id': _req[texts.index(x['text'])]['id']} for x in resp]
            resp_list.append(r)

        result[n_sentence] = resp_list

    with DB() as db:
        for key, resp_list in result.items():
            #col = 'score_{}'.format(key) if 'sentiment' in mode else 'keywords_{}'.format(key)
            col = 'polarity' if 'polarity' in mode else 'keywords'

            for resp in resp_list:
                args = tuple(zip([decide_polarity(x['annotations']['polarity'][0]['value']) if len(x['annotations']) > 0 else 0 for x in resp],
                                             [x['id'] for x in resp]))
                db.custom_put_many('update scores_isp set {}=%s where comment_id=%s'.format(col), args)

def decide_polarity(input):
    if 'NEGATIVE' in input:
        return -1
    elif 'POSITIVE' in input:
        return 1


def db2csv():
    with DB() as db:
        data = db.custom_get('''
                            select wtf.user,
                            wtf.bdate,
                            wtf.date,
                            scores.score_1 as sentiment,
                            #wtf.city,
                            scores.keywords_full as keywords,
                            wtf.text
                                from scores
                                    join
                                        (select stalin.id as comment_id, stalin.date, stalin.text, users.id as user, users.* from stalin join users on stalin.user_id=users.id)
                                            as wtf
                                    on scores.comment_id=wtf.comment_id
                                        where score_1 is not null
                                            order by wtf.date''', ( ))

    lines = []
    lines.append('{}\n'.format('$$$'.join(['user_id', 'birthday', 'datetime', 'sentiment score', 'keywords', 'text'])))
    for row in data:
        #lines.append('{}\n'.format('$$$'.join([sub('\W', ' ', str(x)) for x in row])))
        lines.append('{}\n'.format(sub('\n', ' ', row[-1])))
    with open('output_text.txt', 'w', encoding='utf-8') as f:
        f.write(''.join(lines))


if __name__ == '__main__':
    main_isp()
    #db2csv()