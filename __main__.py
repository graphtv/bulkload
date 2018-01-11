import argparse
import sys
import json
import boto3
import zlib

from bulkload import *
from argparse import RawTextHelpFormatter

_logger = logger.get_logger(__name__)
parser = argparse.ArgumentParser(
    description='The GraphTV BulkLoad tool parses data from IMDB exports, transforms them into easily consumable \
    datatypes and bulk loads them.',
    formatter_class=RawTextHelpFormatter
)
parser.add_argument(
    '--source-dir',
    default=os.environ.get('GRAPHTV_SOURCE_DIR'),
    metavar='GRAPHTV_SOURCE_DIR',
    help='IMDB Export Source Directory'
)
parser.add_argument(
    '--ratings-table',
    default=os.environ.get('GRAPHTV_RATINGS_TABLE'),
    metavar='GRAPHTV_RATINGS_TABLE',
    help='DynamoDB Ratings Table'
)

def main(args=None):
    _logger.info("GraphTV Bulk Load - Version 0.1")
    args = parser.parse_args()
    '''
    _logger.info("Reading IMDB Exports from TSV: %s", args.source_dir)
    importFiles = {
        'title': os.path.join(args.source_dir, 'title.basics.tsv\data.tsv'),
        'episode': os.path.join(args.source_dir, 'title.episode.tsv\data.tsv'),
        'ratings': os.path.join(args.source_dir, 'title.ratings.tsv\data.tsv'),
    }
    
    # Load data from TSV files
    loader = Loader()
    titles = loader.load_titles(importFiles['title'])
    episodes = loader.load_episodes(importFiles['episode'])
    ratings = loader.load_ratings(importFiles['ratings'])
    
    # Export TSV contents to JSON files
    with open(os.path.join(args.source_dir, 'temp', 'titles.json'), 'w', encoding='utf-8') as f:
        json.dump(titles, f, ensure_ascii=False, separators=(',', ':'))
    with open(os.path.join(args.source_dir, 'temp', 'episodes.json'), 'w', encoding='utf-8') as f:
        json.dump(episodes, f, ensure_ascii=False, separators=(',', ':'))
    with open(os.path.join(args.source_dir, 'temp', 'ratings.json'), 'w', encoding='utf-8') as f:
        json.dump(ratings, f, ensure_ascii=False, separators=(',', ':'))
    
    # Load Data from JSON files
    _logger.info("Reading IMDB Exports from JSON: %s", os.path.join(args.source_dir, 'temp'))
    with open(os.path.join(args.source_dir, 'temp', 'titles.json'), 'r', encoding='utf-8') as f:
        titles = json.load(f)
        _logger.info("     Loaded Titles")
    with open(os.path.join(args.source_dir, 'temp', 'episodes.json'), 'r', encoding='utf-8') as f:
        episodes = json.load(f)
        _logger.info("     Loaded Episodes")
    with open(os.path.join(args.source_dir, 'temp', 'ratings.json'), 'r', encoding='utf-8') as f:
        ratings = json.load(f)
        _logger.info("     Loaded Ratings")
    
    # Merge JSON files into single JSON object with shows, episodes, and ratings
    merger = Merger()
    merged = merger.merge(titles, episodes, ratings, is_compact=True)
    with open(os.path.join(args.source_dir, 'temp', 'merged.json'), 'w', encoding='utf-8') as f:
        # Minimized JSON
        json.dump(merged, f, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
    with open(os.path.join(args.source_dir, 'temp', 'merged_readable.json'), 'w', encoding='utf-8') as f:
        # Human Readable JSON
        json.dump(merged, f, ensure_ascii=False, indent=4, sort_keys=True)
    _logger.info("Reading Merged Ratings from JSON: %s", os.path.join(args.source_dir, 'temp'))
    
    # Load merged data from JSON file
    with open(os.path.join(args.source_dir, 'temp', 'merged.json'), 'r', encoding='utf-8') as f:
        merged = json.load(f)
        _logger.info("     Loaded Merged Ratings")

    # Send merged data to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(args.ratings_table)
    with table.batch_writer() as batch:
        last_percent = 0
        total_shows = len(merged)
        cur_show_num = 0
        for show_id, show_obj in merged.items():
            data_json = json.dumps(show_obj, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
            data_zlib = zlib.compress(data_json.encode(), 9)
            batch.put_item(
                Item={
                    'id': show_id,
                    'data': (data_json if sys.getsizeof(data_json) <= sys.getsizeof(data_zlib) else data_zlib)
                }
            )
            '''
            '''
            # Print progress output
            cur_show_num += 1
            current_percent = math.floor(cur_show_num / total_shows * 100)
            if current_percent > last_percent:
                last_percent = current_percent
                _logger.info("%s/%s (%s%%) Complete...", cur_show_num, total_shows, current_percent)
                
    # Misc analysis output trying to gauge sizes of queried data
    size_dict = {}
    for x in range(0, 520):
        size_dict[x] = 0
    for show_id, show_obj in merged.items():
        cur_json = json.dumps(show_obj, ensure_ascii=False, separators=(',', ':'))
        cur_comp = zlib.compress(cur_json.encode(), 9)
        #cur_comp = zlib.compress(cur_json.encode(), 6)
        #cur_comp = zlib.compress(cur_json.encode(), 2)
        #cur_comp = cur_json
        cur_size = sys.getsizeof(cur_comp)
        cur_size_k = math.ceil(cur_size / 1024)
        for ep_id, ep_obj in show_obj['l'].items():
            size_dict[cur_size_k] += (ep_obj['v'] if 'v' in ep_obj else 0)
    for size_k, vote_count in size_dict.items():
        print("{}\t{}".format(size_k, vote_count))

    # Unknown?
    kdfjk
    with open(os.path.join(args.source_dir, 'temp', 'final.tsv'), 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['show', 'data']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        for show_id, show_obj in merged.items():
            writer.writerow({
                'show': show_id,
                'data': json.dumps(show_obj, ensure_ascii=False, separators=(',', ':'))
            })
            
    # Unknown
    new_dict = {}
    for title_id, title_obj in titles.items():
        new_dict[title_obj['primaryTitle']] = title_id
    with open(os.path.join(args.source_dir, 'temp', 'only_titles.json'), 'w', encoding='utf-8') as f:
        json.dump(new_dict, f, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    _logger.info("Done!")
    '''
    _logger.info("Exited.")


if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()