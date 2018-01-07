import argparse
import sys
import json

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
    loader = Loader()
    titles = loader.load_titles(importFiles['title'])
    episodes = loader.load_episodes(importFiles['episode'])
    ratings = loader.load_ratings(importFiles['ratings'])
    with open(os.path.join(args.source_dir, 'temp', 'titles.json'), 'w', encoding='utf-8') as f:
        json.dump(titles, f, ensure_ascii=False, separators=(',', ':'))
    with open(os.path.join(args.source_dir, 'temp', 'episodes.json'), 'w', encoding='utf-8') as f:
        json.dump(episodes, f, ensure_ascii=False, separators=(',', ':'))
    with open(os.path.join(args.source_dir, 'temp', 'ratings.json'), 'w', encoding='utf-8') as f:
        json.dump(ratings, f, ensure_ascii=False, separators=(',', ':'))
    '''
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
    merger = Merger()
    merged = merger.merge(titles, episodes, ratings)
    _logger.info("Exited.")



if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()