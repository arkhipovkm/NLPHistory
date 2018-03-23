from machinery.audiodbclassMY import DB
from machinery import audiorequests as arq

def get_hashids():
    with DB() as db:
        hashids = db.custom_get('select hash, song_id from audio join users on audio.owner=users.vk_id where is_incloud=0 and num-count < -1000', ())
    return hashids

def chunk_hashids(hashids):
    chunks = []
    k = 0
    for n in range(len(hashids)//1000 + 1):
        chunk = hashids[k*1000:(n+1)*1000]
        chunks.append(chunk)
        k += 1
    return chunks

def prepare_requests(hashids):
    hashids_dict = {}
    seen = set()
    for hashid in hashids:
        vk_id = hashid[1].split('_')[0]
        if vk_id not in seen:
            hashids_dict[vk_id] = []
            seen.add(vk_id)
        else:
            hashids_dict[vk_id].append(hashid)
    return hashids_dict

def make_requests(hashids_dict):
    for key, value in hashids_dict.items():
        vk_id = int(key)
        hashids = value
        arq.reload_audio_custom(vk_id, hashids)



def get_hashurls(chunks):
    for chunk in chunks:
        arq.