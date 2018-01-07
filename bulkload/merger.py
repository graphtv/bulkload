import bulkload
import math


class Merger:
    _logger = bulkload.logger.get_logger(__name__)

    def merge(self, titles, episodes, ratings):
        self._logger.info("Beginning episode merge...")
        last_percent = 0
        cur_show_num = 0
        combined = {}
        for show_id, show_obj in titles.items():
            cur_show_num += 1
            if cur_show_num == 100:
                self._logger.info("%s/%s Complete...", cur_show_num, len(titles))
            current_percent = math.floor(cur_show_num / len(titles))
            if current_percent > last_percent:
                last_percent = current_percent
                self._logger.info("%s/%s (%s) Complete...", cur_show_num, len(titles), current_percent)
            '''
            if show_id != 'tt0903747':
                # Temporarily limit testing to just combining Breaking Bad episodes
                continue
            '''
            if show_id not in combined:
                combined[show_id] = {}
                combined[show_id]['title'] = show_obj['primaryTitle']
                combined[show_id]['episodes'] = {}
            for episode_id, episode_obj in episodes.items():
                if episode_obj['parentId'] == show_id:
                    if episode_id in ratings:
                        rating = ratings[episode_id]['rating']
                        votes = ratings[episode_id]['votes']
                    else:
                        rating = -1
                        votes = 0
                    combined[show_id]['episodes'][episode_id] = {
                        'title': titles[episode_id]['primaryTitle'],
                        'season': episode_obj['season'],
                        'episode': episode_obj['episode'],
                        'rating': rating,
                        'votes': votes
                    }
        return combined