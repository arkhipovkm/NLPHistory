from nlpdb import DB
import json

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
        rubric_comments.append(res)
        print('Acquired comments for rubric: {}'.format(rubric))
    return rubric_comments

def main():
    rubrics = get_rubrics()
    result = []
    for rubric in rubrics:
        comments = get_comments(rubric)
        result.append({'rubric': rubric, 'count': len(comments), 'comments': comments})
    with open('result_rubrics_comments.json', 'w') as f:
        json.dump(result, f)

if __name__ == '__main__':
    main()