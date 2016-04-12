import logging, ConfigParser, os, time, datetime, codecs, json, sys
import hermes.backend.redis, bs4, unicodecsv
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

@cache
def api_story_count(query,filter_query):
    return mc.storyCount(query,filter_query)

@cache
def api_story_list(query, filter_query, start, offest):
    return mc.storyList(solr_query=query, solr_filter=filter_query, 
            last_processed_stories_id=start, rows=offset, raw_1st_download=True)

def zi_time(d):
    return datetime.datetime.combine(d, datetime.time.min).isoformat() + "Z"

def build_date_query_part(start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    return '(publish_date:[{0} TO {1}])'.format(zi_time(start_date), zi_time(end_date))

MEDIA_SOURCE_IDS = config.get("project","media_sources").split(",")
logger.info("Reading from {0} media sources".format(len(MEDIA_SOURCE_IDS)))

QUERY = config.get('project','query')
date_range_query = build_date_query_part(config.get('project','start_date'),config.get('project','end_date'))

# print out story counts
total_story_count = api_story_count(QUERY, 
    ["media_id:"+" ".join(MEDIA_SOURCE_IDS),date_range_query])['count']
logger.info("Found {0} total stories".format(total_story_count))
media_sources = []
for media_id in MEDIA_SOURCE_IDS:
    media_source = mc.media(media_id)
    media_story_count = api_story_count(QUERY, 
        ["media_id:"+media_id,date_range_query])['count']
    logging.info("  {0}: {1} stories".format(media_source['name'], media_story_count))
    media_sources.append(media_source)

# set up a csv to record all the story images
image_url_csv_file = open(os.path.join(basedir,'image_urls.csv'), 'w')
fieldnames = ['media_source', 'pub_date', 'image_url', 'mediacloud_stories_id', 'story_url']
image_url_csv = unicodecsv.DictWriter(image_url_csv_file, fieldnames = fieldnames, 
    extrasaction='ignore', encoding='utf-8')
image_url_csv.writeheader()

image_count = 0

# query MC for stories and  grab raw html content
for source in media_sources:
    logger.info("Fetching stories from {0}".format(source['name']) )
    start = 0
    offset = 500
    page = 0
    while True:
        logger.info("  page {0}".format(page))
        query_start_time = time.time()
        stories = api_story_list(QUERY,["media_id:"+media_id,date_range_query], start, offset)
        query_duration = time.time() - query_start_time
        logger.debug("    (fetched in {0} secs)".format(query_duration))
        # extract image urls
        for story in stories:
            html_content = story['raw_first_download_file']
            soup = bs4.BeautifulSoup(html_content, 'html.parser')
            story_images = [elem['src'] for elem in soup.findAll('img')]
            for img_src in story_images:
                # TODO: automatically remove ad domains
                data = {
                    'media_source': source['name'],
                    'pub_date': story['publish_date'],
                    'image_url': img_src,
                    'mediacloud_stories_id': story['stories_id'],
                    'story_url': story['url']
                }
                image_url_csv.writerow(data)
                image_url_csv_file.flush()
                image_count = image_count + 1
        if len(stories) < 1:
            break
        start = max([s['processed_stories_id'] for s in stories])
        page = page + 1
    logger.info('  Retrieved {0} stories for query {1}'.format(len(all_stories), solr_query))

total_duration = time.time() - start_time
logger.info("Done in {0} secs".format(total_duration))

# TODO: manually code for real images or not
