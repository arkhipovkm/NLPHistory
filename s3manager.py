import boto3
import botocore
import json

BUCKET = 'nlphistorys3'
URL = 'https://s3-us-west-2.amazonaws.com/nlphistorys3/'

s3c = boto3.client('s3')
s3r = boto3.resource('s3')

def _s3_upload(body, key, bucket=BUCKET):
    s3r.Bucket(BUCKET).put_object(Key=key, Body=body)
    #s3c.upload_fileobj(fileobj, bucket, key)

def _s3_download(key, download_filename, bucket=BUCKET):
    s3r.Bucket(bucket).download_file(key, download_filename)

def _s3_list():
    obs = []
    for obj in s3r.Bucket(BUCKET).objects.all():
        obs.append(obj._key)
    return obs

def _s3_get_object(key):
    return json.loads(s3c.get_object(Bucket=BUCKET, Key=key)['Body'].read().decode('utf8'))

def _s3_upload_file(fileobj, key):
    s3r.Bucket(bucket).upload_fileobj(fileobj, key)

def _s3_make_public(key, bucket=BUCKET):
    obj = s3r.Object(bucket, key)
    resp = obj.Acl().put(ACL='public-read')
    

'''
def put_s3(fileobj, key):
    key = 'audiodata/' + hash + '.mp3'
    _s3_upload(localname, key)
    _s3_make_public(key)

def get_s3(localname, hash):
    key = 'audiodata/' + hash + '.mp3'
    _s3_download(key, localname)
    '''