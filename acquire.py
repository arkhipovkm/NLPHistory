import requests

def al_search(query, offset):
    url = 'https://vk.com/al_search.php'
    header = {'cookie': 'remixsid=0832055deddbd459703b3b337ff90e834dad374eb7ee8cb5ce3b9'}
    data = {'al': 1,
            'al_ad': 0,
            'c[likes]': 5,
            'c[q]': query,
            'c[section]': 'statuses',
            'c[type]': 1,
            'offset': offset}
    resp = requests.post(url, data=data, headers=header)
