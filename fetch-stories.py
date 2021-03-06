import logging, sys
import ConfigParser
import os
import time
import datetime
import hermes.backend.redis
import bs4
import unicodecsv
import mediacloud

basedir = os.path.dirname(os.path.abspath(__file__))

# set up logging
logging.basicConfig(filename=os.path.join(basedir, 'logs', 'fetch-stories.log'), level=logging.DEBUG)
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
    db=int(config.get('cache', 'redis_db_number')))

# glocal mediacloud connection
mc = mediacloud.api.AdminMediaCloud(config.get('mediacloud', 'key'))

@cache
def api_story_count(query, filter_query):
    return mc.storyCount(query, filter_query)

@cache
def api_story_list(query, filter_query, start_processed_stories_id, offest_rows):
    return mc.storyList(solr_query=query, solr_filter=filter_query,
            last_processed_stories_id=start_processed_stories_id, rows=offest_rows, raw_1st_download=True)

def zi_time(d):
    return datetime.datetime.combine(d, datetime.time.min).isoformat() + "Z"

def build_date_query_part(start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    return '(publish_date:[{0} TO {1}])'.format(zi_time(start_date), zi_time(end_date))

def is_valid_img(url):
    if url.startswith('data'):
        return False
    # TODO: automatically remove ad domains
    return True

MEDIA_SOURCE_IDS = config.get("project", "media_sources").split(",")
logger.info("Reading from {0} media sources".format(len(MEDIA_SOURCE_IDS)))

QUERY = config.get('project', 'query')
date_range_query = build_date_query_part(config.get('project', 'start_date'), config.get('project', 'end_date'))

# set up a csv to record all the story counts
story_count_csv_file = open(os.path.join(basedir, 'media_story_counts.csv'), 'w')
fieldnames = ['media_id', 'media_source', 'stories_matching', 'stories_total']
story_count_csv = unicodecsv.DictWriter(story_count_csv_file, fieldnames=fieldnames,
    extrasaction='ignore', encoding='utf-8')
story_count_csv.writeheader()

# print out story counts
total_story_count = api_story_count(QUERY,
    ["media_id:"+" ".join(MEDIA_SOURCE_IDS), date_range_query])['count']
logger.info("Found {0} total stories".format(total_story_count))
media_sources = []
for media_id in MEDIA_SOURCE_IDS:
    media_source = mc.media(media_id)
    media_all_story_count = api_story_count('*', ["media_id:"+media_id, date_range_query])['count']
    media_story_count = api_story_count(QUERY, ["media_id:"+media_id, date_range_query])['count']
    data = {
        'media_id': media_source['media_id'],
        'media_source': media_source['name'],
        'stories_matching': media_story_count,
        'stories_total': media_all_story_count
    }
    story_count_csv.writerow(data)
    story_count_csv_file.flush()
    logging.info("  {0}: {1} stories".format(media_source['name'], media_story_count))
    media_sources.append(media_source)

# set up a csv to record all the stories
story_url_csv_file = open(os.path.join(basedir, 'story_urls.csv'), 'w')
fieldnames = ['media_id', 'media_source', 'pub_date', 'mediacloud_stories_id', 'story_title', 'story_url']
story_url_csv = unicodecsv.DictWriter(story_url_csv_file, fieldnames=fieldnames,
    extrasaction='ignore', encoding='utf-8')
story_url_csv.writeheader()

# set up a csv to record all the story images
image_url_csv_file = open(os.path.join(basedir, 'image_urls.csv'), 'w')
fieldnames = ['media_source', 'pub_date', 'image_url', 'image_width', 'image_height', 'mediacloud_stories_id', 'story_url']
image_url_csv = unicodecsv.DictWriter(image_url_csv_file, fieldnames=fieldnames,
    extrasaction='ignore', encoding='utf-8')
image_url_csv.writeheader()

image_count = 0
story_count = 0

# query MC for stories and  grab raw html content
for source in media_sources:
    logger.info("Fetching stories from {0}".format(source['name']))
    start = 0
    offset = 500
    page = 0
    media_image_count = 0
    media_story_count = 0
    media_invalid_image_count = 0
    while True:
        logger.info("  page {0}".format(page))
        query_start_time = time.time()
        stories = api_story_list(QUERY, ["media_id:{0}".format(source['media_id']), date_range_query], start, offset)
        query_duration = time.time() - query_start_time
        logger.debug("    (fetched {0} stories in {1} secs)".format(len(stories), query_duration))
        # extract image urls
        extract_start = time.time()
        for story in stories:
            # add story to CSV file
            data = {
                'media_id': source['media_id'],
                'media_source': source['name'],
                'pub_date': story['publish_date'],
                'mediacloud_stories_id': story['stories_id'],
                'story_title': story['title'],
                'story_url': story['url']
            }
            story_url_csv.writerow(data)
            image_url_csv_file.flush()
            # add image to CSV file
            html_content = story['raw_first_download_file']
            soup = bs4.BeautifulSoup(html_content, 'html.parser')
            image_elements = [elem for elem in soup.findAll('img') if elem.has_attr('src')]
            for img_elem in image_elements:
                img_src = img_elem['src']
                img_width = img_elem['width'] if img_elem.has_attr('width') else ''
                img_height = img_elem['height'] if img_elem.has_attr('height') else ''
                if is_valid_img(img_src):
                    data = {
                        'media_source': source['name'],
                        'pub_date': story['publish_date'],
                        'image_url': img_src,
                        'image_width': img_width,
                        'image_height': img_height,
                        'mediacloud_stories_id': story['stories_id'],
                        'story_url': story['url']
                    }
                    image_url_csv.writerow(data)
                    image_url_csv_file.flush()
                    media_image_count = media_image_count + 1
                else:
                    media_invalid_image_count = media_invalid_image_count + 1
            media_story_count = media_story_count + 1
        extract_duration = time.time() - extract_start
        logger.debug("    (extracted in {0} secs)".format(extract_duration))
        if len(stories) < 1:
            break
        start = max([s['processed_stories_id'] for s in stories])
        page = page + 1
    logger.info("{0}: Retrieved {1} stories with {2} images ({3} invalid)".format(
        source['name'], media_story_count, media_image_count, media_invalid_image_count))
    image_count = image_count + media_image_count
    story_count = story_count + media_story_count

total_duration = time.time() - start_time
logger.info("Done in {0} secs".format(total_duration))
logger.info("  {0} stories".format(story_count))
logger.info("  {0} images".format(image_count))
