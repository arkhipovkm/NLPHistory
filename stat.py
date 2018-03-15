from nlpdb import DB
from acquire_comments import groups_getById as get_names
import json

db = DB()
groups_raw = db.custom_get('select * from last_offsets', ())
groups_total = [(x[0], x[-1]) for x in groups_raw]
group_ids = ','.join([str(x[0]) for x in groups_raw])
group_names = get_names(group_ids)
groups = [(y['id'], y['name'], [x[-1] for x in groups_raw if x[0] == y['id']][0]) for y in group_names]
COUNTS = []
for group in groups:
    group_name = group[1]
    group_id = group[0]
    total = group[-1]

    count = db.custom_get('select count(*) from nlp_isam.comments_isam where group_id=%s', (group_id, ))[0][0]
    posts = db.custom_get('select count(distinct post_id) from nlp_isam.comments_isam where group_id=%s', (group_id, ))[0][0]

    count_2017 = db.custom_get('select count(*) from nlp_isam.comments_isam where group_id=%s and date > "20170101" and date < "20171231"', (group_id, ))[0][0]
    count_2016 = db.custom_get('select count(*) from nlp_isam.comments_isam where group_id=%s and date > "20160101" and date < "20161231"', (group_id, ))[0][0]
    count_2015 = db.custom_get('select count(*) from nlp_isam.comments_isam where group_id=%s and date > "20150101" and date < "20151231"', (group_id, ))[0][0]

    output = {group_name: {
            'overall': {
                 'total_comments': count,
                 'percentage': '{:.2f}'.format(posts/total) },
            'years': {
                '2017': count_2017,
                '2016': count_2016,
                '2015': count_2015 }
            }
         }

    print(output)

    COUNTS.append(output)
with open('stats_nlp.json', 'w') as f:
    json.dump(COUNTS, f)