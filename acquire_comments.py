#import os
#os.chdir('F:\\Users\\Kirill\\source\\repos\\NLPHistory')
import requests
from nlpdb import DB
from time import sleep

__marianne_token__ = '6faaf0f33949745684fdf0a59dfac78bd41562a3da95c009c0fb155458fa55b0d0199701f3309b9878d19' #WALL, OFFLINE
__marianne_id__ = 395888840

__encryptify_token__ = '463989315e451575bf4d06b185ebda53b10ccd33438c95f9dbe27af4dadfe19897dfd326181ad9a97bd6d'
__encryptify_id__ = 133644218

__service_token__ = 'd556709dd556709dd556709de2d53779b1dd556d556709d8f21592aa71dfeb6a97cab4e'

token = __marianne_token__
#token = __encryptify_token__
vk_id = __marianne_id__

get_user_token_url = 'https://oauth.vk.com/authorize?client_id=6359340&redirect_uri=https://oauth.vk.com/blank.html&response_type=token&scope=wall'
get_group_token_url = 'https://oauth.vk.com/authorize?client_id=6359340&redirect_uri=https://oauth.vk.com/blank.html&response_type=token&scope=manage&group_ids=133644218'

URL = 'https://api.vk.com/method/'

class ResponseError(Exception):
    def __init__(self, resp):
        try:
            msg = 'Got the response with error: {}'.format(resp['error']['error_msg'])
        except:
            msg = resp
        Exception.__init__(self, msg)

def vkapi(func):
    def inner(*args, **kwargs):
        params = func(*args, **kwargs)
        url = URL + params #+ 'access_token={}'.format(token)
        resp = requests.get(url).json()
        counter = 0
        while counter < 5:
            try:
                return resp['response']
            except KeyError:
                if 'Too many requests per second' in resp['error']['error_msg']:
                    print('Hit the frequency limit. Retry in 0.5 sec..')
                    sleep(1)
                    counter += 1
                    continue
                else:
                    raise ResponseError(resp)
    raise ResponseError(resp)
    return inner

@vkapi
def groups_get():
    return 'groups.get?v=5.71&access_token={}'.format(__marianne_token__)

@vkapi
def wall_get(group, offset=0):
    return 'wall.get?owner_id=-{}&count=100&v=5.71&offset={}&access_token={}'.format(group, offset, __marianne_token__)

@vkapi
def wall_get_comments(group, post, offset=0):
    return 'wall.getComments?&owner_id=-{}&post_id={}&preview_length=0&need_likes=1&count=100&extended=1&fields=bdate,sex,country,city&offset={}&v=5.71&'.format(group, post, offset)

@vkapi
def execute_get_comments(group, offset=0):
    return 'execute.getComments?group=-{}&offset={}&v=5.71&access_token={}'.format(group, offset, __marianne_token__)


def main(group):

    with DB() as db:
        done = db.get_done()
        doneset = set(x[0] for x in done)

    try:
        posts_count = wall_get(group)['count']
        #posts_count = 20
    except Exception as e:
        print(e)
        raise e

    for n in range(posts_count // 100 + 1):
        try:
            offset = n*100
            posts = wall_get(group, offset=offset)
        except Exception as e:
            print(e)
            raise e

        for post in [x for x in posts['items']
                        if x['comments']['count'] > 0
                        and '{}_{}'.format(group, x['id']) not in doneset]:
            try:
                comments_count = post['comments']['count']
            except Exception as e:
                print(e)
                raise e

            for n in range(comments_count // 100 + 1):
                try:
                    offset = n*100
                    comments = wall_get_comments(group, post['id'], offset=offset)
                except Exception as e:
                    print(e)
                    raise e
                with DB() as db:
                    db.add_comments(group, post['id'], comments['items'])
                    db.add_users(comments['profiles'])
                    db.add_done(group, post['id'], comments_count, offset+len(comments['items']))


def main_stored(group):

    with DB() as db:
        offset = db.get_done_by_group(group)[0][0] - 1

    posts_count = wall_get(group)['count']

    while offset < posts_count + 25:
        response = execute_get_comments(group, offset=offset)
        with DB() as db:
            for item in response['items']:
                db.add_comments(abs(int(item['group'])), int(item['post']), item['comments']['items'])
                db.add_users(item['comments']['profiles'])
                db.add_done(abs(int(item['group'])), int(item['post']), item['comments']['count'], len(item['comments']['items']))
        offset = response['next_offset']


if __name__ == '__main__':

    groups = groups_get()['items']
    #main_stored(groups[0])

    from multiprocessing import Pool
    with Pool(4) as pool:
        pool.starmap(main_stored, [(x,)  for x in groups])