import csv
import bulkload
import time


class Loader:
    _logger = bulkload.logger.get_logger(__name__)

    def load_titles(self, filename):
        return_dict = {}
        start_time = time.time()
        '''
        with gzip.open(
            filename=os.path.join(args.source_dir, 'title.basics.tsv.gz'), mode='rt', newline='', encoding='utf8'
        ) as tsvfile:
        '''
        with open(file=filename, mode='rt', newline='', encoding='utf8') as tsvfile:
            tsv_reader = csv.DictReader(f=tsvfile, delimiter='\t')
            for row in tsv_reader:
                if row['titleType'].startswith('tv'):
                    return_dict[row['tconst']] = {
                        'id': row['tconst'],
                        'titleType': row['titleType'],
                        'primaryTitle': row['primaryTitle'],
                        'originalTitle': row['originalTitle']
                    }
        elapsed_time = time.time() - start_time
        self._logger.info("Loaded {:,} titles in {:.2f} seconds.".format(len(return_dict), elapsed_time))
        return return_dict

    def load_episodes(self, filename):
        return_dict = {}
        start_time = time.time()
        '''
        with gzip.open(
            filename=os.path.join(args.source_dir, 'title.basics.tsv.gz'), mode='rt', newline='', encoding='utf8'
        ) as tsvfile:
        '''
        with open(file=filename, mode='rt', newline='', encoding='utf8') as tsvfile:
            tsv_reader = csv.DictReader(f=tsvfile, delimiter='\t')
            for row in tsv_reader:
                return_dict[row['tconst']] = {
                    'id': row['tconst'],
                    'parentId': row['parentTconst'],
                    'season': int(-1 if row['seasonNumber'] == '\\N' else row['seasonNumber']),
                    'episode': int(-1 if row['episodeNumber'] == '\\N' else row['episodeNumber'])
                }
        elapsed_time = time.time() - start_time
        self._logger.info("Loaded {:,} episodes in {:.2f} seconds.".format(len(return_dict), elapsed_time))
        return return_dict

    def load_ratings(self, filename):
        return_dict = {}
        start_time = time.time()
        '''
        with gzip.open(
            filename=os.path.join(args.source_dir, 'title.basics.tsv.gz'), mode='rt', newline='', encoding='utf8'
        ) as tsvfile:
        '''
        with open(file=filename, mode='rt', newline='', encoding='utf8') as tsvfile:
            tsv_reader = csv.DictReader(f=tsvfile, delimiter='\t')
            for row in tsv_reader:
                return_dict[row['tconst']] = {
                    'id': row['tconst'],
                    'rating': float(row['averageRating']),
                    'votes': int(row['numVotes'])
                }
        elapsed_time = time.time() - start_time
        self._logger.info("Loaded {:,} ratings in {:.2f} seconds.".format(len(return_dict), elapsed_time))
        return return_dict
