#import os
#os.chdir('F:\\Users\\Kirill\\source\\repos\\NLPHistory')
import requests
from nlpdb import DB

__marianne_token__ = '6faaf0f33949745684fdf0a59dfac78bd41562a3da95c009c0fb155458fa55b0d0199701f3309b9878d19' #WALL, OFFLINE
__marianne_id__ = 395888840

token = __marianne_token__
vk_id = __marianne_id__

URL = 'https://api.vk.com/method/'

class ResponseError(Exception):
    def __init__(self, resp):
        try:
            msg = 'Got the response with error: {}'.format(resp['error']['error_msg'])
        except:
            print(resp)
        Exception.__init__(self, msg)

def vkapi(func):
    def inner(*args, **kwargs):
        params = func(*args, **kwargs)
        url = URL + params + 'access_token={}'.format(token)
        resp = requests.get(url).json()
        try:
            return resp['response']
        except KeyError:
            raise ResponseError(resp)
    return inner

@vkapi
def groups_get():
    return 'groups.get?v=5.71&'

@vkapi
def wall_get(group, offset=0):
    return 'wall.get?owner_id=-{}&count=100&v=5.71&offset={}&'.format(group, offset)

@vkapi
def wall_get_comments(group, post, offset=0):
    return 'wall.getComments?&owner_id=-{}&post_id={}&preview_length=0&need_likes=1&count=100&extended=1&fields=bdate,sex,country,city&offset={}&v=5.71&'.format(group, post, offset)

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

if __name__ == '__main__':

    groups = groups_get()['items']

    from multiprocessing import Pool
    with Pool(2) as pool:
        pool.starmap(main, [(x,)  for x in groups])