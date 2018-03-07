from nlpdb import DB
import nltk
from nltk.corpus import stopwords
from pymorphy2 import MorphAnalyzer
from re import sub
import string
from collections import Counter
import json

morph = MorphAnalyzer()

def main(year):

    stop_words = stopwords.words('russian')
    stop_words.extend(['что', 'че', 'чё' 'это', 'так', 'вот', 'быть', 'как', 'в', '—', 'к', 'на', 'если', '""', "''", '``'])

    with DB() as db:
        comments = db.custom_get('''select * from {0}_100k where date > {1}0101 and date < {1}1231'''.format(category, year), ( ))
    total_comments = len(comments)

    result = []
    for row in comments:
        #polarity = row[2]
        comment_id = row[0]
        comment = row[5].lower()
        comment = sub('\[id[0-9]*?\|.*?\]', '', comment)

        tokens = nltk.word_tokenize(comment)
        tokens = [i for i in tokens if ( i not in string.punctuation )] #Eliminate punctuation
        tokens = [i for i in tokens if ( i not in stop_words )] #Eliminate stop_words
        tokens = [sub('\W', '', i) for i in tokens] #Eliminate non-words
        tokens = [i for i in tokens if i] #Eliminate '' and ""

        tokens_pos = nltk.pos_tag(tokens, lang='rus') # Detect POS of tokens and return tuple (token, POS)
        tokens = [i[0] for i in tokens_pos if (i[1] in ['V', 'S'] or 'A=' in i[1] or 'NUM' in i[1])] #Return only nouns and verbs ???
        #root_idxs = [tokens.index(i) for i in tokens if root_word in i]
        #tokens = [tokens[i-n:i+n] for i in root_idxs]

        # Flatten token lists of lists
        '''
        flatten = []
        for i in tokens:
            for x in i:
                flatten.append(x)
        tokens = flatten
        '''

        #tokens = [i for i in sublist for sublist in tokens]
        #normal_tokens = [morph.parse(i)[0].inflect({'sing', 'nomn'}).word for i in tokens]

        normal_tokens = [morph.normal_forms(i)[0] for i in tokens] #Lemmetize (returns normal form
        result += normal_tokens
        #result.append({'id': comment_id, 'text': comment, 'tokens': normal_tokens, 'polarity': polarity})
    return result, total_comments

if __name__ == '__main__':
    for category in ['sssr', 'lenin', 'revolution']:
        for year in [2015, 2016, 2017]:
            result, total_comments = main(year)
            unique_words = len(result)
            c = Counter(result)
            common = c.most_common(30)
            with open('_result_frequencies_115k_[{}_{}_vna-num].json'.format(year, category), 'w') as f:
                json.dump({'total_comments': total_comments, 'unique_words': unique_words, 'result': common}, f)