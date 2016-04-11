import logging, ConfigParser, os, time, datetime
import hermes.backend.redis
import mediacloud

basedir = os.path.dirname(os.path.abspath(__file__))

# set up logging
logging.basicConfig(filename=os.path.join(basedir,'logs','fetch-stories.log'),level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.info("---------------------------------------------------------------------------")
start_time = time.time()
requests_logger = logging.getLogger('requests')
requests_logger.setLevel(logging.WARN)
mc_logger = logging.getLogger('mediacloud')
mc_logger.setLevel(logging.WARN)

# load the settings file
config = ConfigParser.ConfigParser()
config.read(os.path.join(basedir, 'app.config'))

# initialize the cache (expires in 1 year)
cache = hermes.Hermes(hermes.backend.redis.Backend, ttl=31104000, host='localhost', 
    db=int(config.get('cache','redis_db_number')))

# glocal mediacloud connection
mc = mediacloud.api.AdminMediaCloud(config.get('mediacloud','key'))

def fetch_story_count(query,filter_query):
    return mc.storyCount(query,filter_query)

def zi_time(d):
    return datetime.datetime.combine(d, datetime.time.min).isoformat() + "Z"

def build_date_query_part(start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    return '(publish_date:[{0} TO {1}])'.format(zi_time(start_date), zi_time(end_date))

MEDIA_SOURCE_IDS = config.get("project","media_sources").split(",")
logger.info("Reading from %d media sources" % len(MEDIA_SOURCE_IDS))

QUERY = config.get('project','query')
date_range_query = build_date_query_part(config.get('project','start_date'),config.get('project','end_date'))

DESTINATION = os.path.join(basedir,config.get('project','destination'))
logging.info("Will write stories to %s" % DESTINATION)

# print out story counts
total_story_count = fetch_story_count(QUERY, 
    ["media_id:"+" ".join(MEDIA_SOURCE_IDS),date_range_query])['count']
logger.info("Found %s total stories" % total_story_count)
for media_id in MEDIA_SOURCE_IDS:
    media_source = mc.media(media_id)
    media_story_count = fetch_story_count(QUERY, 
        ["media_id:"+media_id,date_range_query])['count']
    logging.info("  %s: %s stories" % (media_source['name'], media_story_count))

# query MC for story urls
# grab raw html content
# extract image urls
# automatically remove ad domains
# manually code for real images or not
