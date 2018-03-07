import requests
import json

KEY = '63e75cbccf7a1cbd88258c5217ca59331310d2f3'

URL = 'http://api.ispras.ru/texterra/v1/nlp?'

def polarity():
    url = URL + 'targetType=polarity&tweet=true&apikey={}'.format(KEY)
    comment = 'СТАЛИН ГЕРОЙ РОССИИ! ВЫ ЕЩЕ НА КАДЫРОВА НАЧНИТЕ НАЕЗЖАТЬ! ОХРЕНЕЛИ, ПОДПЕНДОСНИКИ ПОХАНЫЕ!'
    comments = ['СТАЛИН ГЕРОЙ РОССИИ! ВЫ ЕЩЕ НА КАДЫРОВА НАЧНИТЕ НАЕЗЖАТЬ! ОХРЕНЕЛИ, ПОДПЕНДОСНИКИ ПОХАНЫЕ!', 'mentioning veterens care which Mccain has voted AGAINST - SUPER GOOOOD point Obama+1 #tweetdebate']
    data = [{'text': comment}]
    data = [{'text': x} for x in comments]
    js = json.dumps(data)
    #js = "[ { \"text\" : \"mentioning veterens care which Mccain has voted AGAINST - SUPER GOOOOD point Obama+1 #tweetdebate\" } ]"
    resp = requests.post(url, data=js, headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
    return resp.json()

if __name__ == '__main__':
    polarity()