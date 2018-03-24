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
        rubric_comments += res
        print('Acquired comments for rubric: {}'.format(expr))
    return rubric_comments

def main():
    rubrics = get_rubrics()
    result = []
    total = 0
    for rubric in rubrics:
        comments = get_comments(rubric)
        #result.append({'rubric': rubric, 'count': len(comments), 'comments': comments})
        result.append({'rubric': rubric, 'count': len(comments)})
        total += len(comments)
    result_dict = {'total': total, 'rubrics': result}

    with open('result_rubrics_comments_dict.json', 'w') as f:
        json.dump(result_dict, f)

if __name__ == '__main__':
    main()