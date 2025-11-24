import argparse, os, time, random, json, datetime, logging, uuid, string

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema.schema import ALLOWED_EVENT_TYPES, DEVICE_POOLS, GEO_POOLS, SAMPLE_USERS, DEFAULT_PROPS_BY_EVENT, SCHEMA_DICT


logging.basicConfig(
    filename='producer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

parser = argparse.ArgumentParser(description='Synthetic event generator for customer behaviour engine')

parser.add_argument('--rate',type=int,default=100,help='events per second')
parser.add_argument('--batch-size',type=int,default=1000,help='events per NDJSON file')
parser.add_argument('--count',type=int,default=None,help='total events to generate')
parser.add_argument('--duration',type=int,default=None,help='run for N seconds')
parser.add_argument('--out-dir',type=str,default='./data/raw',help='file path')
parser.add_argument('--seed',type=int,default=None,help='Optional random seed')


args = parser.parse_args()

rate = args.rate
batch_size = args.batch_size
count = args.count
duration = args.duration
out_dir = args.out_dir
seed = args.seed


if count is not None and duration is not None:
    logging.info('Duration provided but will be ignored because count was specified.')
    logging.info(f"Running in count mode — will generate {count} events.")
elif count is None and duration is not None:
    logging.info(f"Running in duration mode — {duration} seconds.")
elif count is None and duration is None:
    logging.info("Running indefinitely until interrupted.")



logging.info(f'the chosen rate is: {rate}')
logging.info(f'batch size: {batch_size}')
logging.info(f'Out directory: {out_dir}')
logging.info(f'seed: {seed}')



def init_pools(seed):
    if seed is not None:
        random.seed(seed)

    pools = {
        'event_types':ALLOWED_EVENT_TYPES,
        'users':SAMPLE_USERS,
        'device_types':DEVICE_POOLS['device_types'],
        'oses':DEVICE_POOLS['oses'],
        'browsers':DEVICE_POOLS['browsers'],
        'countries':GEO_POOLS['countries'],
        'regions':GEO_POOLS['regions'],
        'cities':GEO_POOLS['cities'],
        'default_props':DEFAULT_PROPS_BY_EVENT
    }
    return pools
    
session_map = {}
session_counts = {}

def get_session_id(user_id,session_map,session_counts):
    if user_id in session_map:
        session_counts[user_id]+=1
        if session_counts[user_id]>20:
            session_id = str(uuid.uuid4())
            session_map[user_id] = session_id
            session_counts[user_id]=1
            return session_id
        else:
            return session_map[user_id]
    else:
        session_id=str(uuid.uuid4())
        session_map[user_id]=session_id
        session_counts[user_id]=1
        return session_id


def build_event(pools, session_map, session_counts):
    user_id = random.choice(pools['users'])
    session_id = get_session_id(user_id, session_map, session_counts)
    event_type = random.choice(pools['event_types'])
    iso_ts = datetime.datetime.now(datetime.timezone.utc)
    event_ts= f'{iso_ts.isoformat()}Z'
    event_id = str(uuid.uuid4())
    device = {
        'device_type':random.choice(pools['device_types']),
        'os':random.choice(pools['oses']),
        'browser':random.choice(pools['browsers'])
    }
    geo = {
        'country':random.choice(pools['countries']),
        'region':random.choice(pools['regions']),
        'city':random.choice(pools['cities']),
        'ip':f'{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}'
    }
    props = {}
    props_keys = pools['default_props'][event_type]
    for i in props_keys:
        if i=='search_query':
            props[i]=random.choice(['shoes','books','laptop','music','news','football','cricket'])
        elif i=='video_id':
            props[i]=f'video_{random.randint(1,100)}'
        elif i=='duration':
            props[i]=random.randint(1,100)
        elif i=='clicked_element':
            props[i]=random.choice(['button','card','banner'])
        elif i=='content_category':
            props[i]=random.choice(['tech','sports','finance'])
        elif i=='product_id':
            props[i]=f'product_{random.randint(1,100)}'
        elif i=='amount':
            props[i]=random.randint(100,3000)
    
    user_props = {
    "plan": random.choice(["free", "premium"]),
    "signup_date": f'{random.randint(2020,2025)}-{random.randint(1,12)}-{random.randint(1,28)}',
    "referral_source": random.choice(["organic", "ads", "email"])
    }

    page = random.choice(["home", "product_page", "search_results", "video_page"])
            
    event = {
    "event_ts": event_ts,
    "event_id": event_id,
    "user_id": user_id,
    "session_id": session_id,
    "event_type": event_type,
    "page": page,
    "device": device,
    "geo": geo,
    "props": props,
    "user_props": user_props
    }
    return event



def write_batch(events, out_dir):
    da = datetime.date.today()
    ti = datetime.datetime.now().hour
    path = f'{out_dir}/date={da}/hour={ti}/'
    os.makedirs(path, exist_ok=True)
    iso_ts = datetime.datetime.now(datetime.timezone.utc)
    t = iso_ts.isoformat()
    safe_t = t.replace(':','-').replace('+','_')
    filename = f'events_{safe_t}.ndjson'
    full_path = os.path.join(path,filename)
    with open(full_path,'w') as f:
        for event in events:
            f.write(json.dumps(event))
            f.write('\n')
    return full_path
    

pools = init_pools(seed)
session_map = {}
session_counts = {}
batch = []
start_time = time.time()
event_generated_count = 0


while True:
    event = build_event(pools, session_map, session_counts)
    batch.append(event)
    event_generated_count +=1
    if len(batch)>=batch_size:
        write_batch(batch, out_dir)
        batch = []
    if count is not None and event_generated_count >= count:
        break 
    current_time = time.time()
    if duration is not None and (current_time-start_time) >= duration:
        break
    sleep_duration = 1/rate
    time.sleep(sleep_duration)

if len(batch)>0:
    write_batch(batch, out_dir)


logging.info('Generator stopped gracefully')