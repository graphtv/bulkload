import bulkload
import math


class Merger:
    _logger = bulkload.logger.get_logger(__name__)

    def merge(self, titles, episodes, ratings, is_compact):
        self._logger.info("Beginning episode merge...")
        if is_compact:
            k_title = 't'
            k_episode_list= 'l'
            k_episode = 'e'
            k_season = 's'
            k_rating = 'r'
            k_votes = 'v'
        else:
            k_title = 'title'
            k_episode_list= 'episodes'
            k_episode = 'episode'
            k_season = 'season'
            k_rating = 'rating'
            k_votes = 'votes'
        last_percent = 0
        total_shows = len(titles)
        cur_show_num = 0
        combined = {}
        for show_id, show_obj in titles.items():
            cur_show_num += 1
            current_percent = math.floor(cur_show_num / total_shows * 100)
            if current_percent > last_percent:
                last_percent = current_percent
                self._logger.info("%s/%s (%s%%) Complete...", cur_show_num, total_shows, current_percent)
            '''
            if show_id != '3N6z':
                # Temporarily limit testing to just combining Breaking Bad episodes
                continue
            '''
            if show_id not in episodes:
                # Show has no episodes
                continue
            if show_id not in combined:
                comb_show = {
                    k_title: show_obj['primaryTitle'],
                    k_episode_list: {}
                }
                vote_count = 0
                for episode_id, episode_obj in episodes[show_id].items():
                    comb_show[k_episode_list][episode_id] = {}
                    if episode_id in ratings:
                        comb_show[k_episode_list][episode_id][k_rating] = ratings[episode_id]['rating']
                        comb_show[k_episode_list][episode_id][k_votes] = ratings[episode_id]['votes']
                        vote_count += ratings[episode_id]['votes']
                    '''
                    # Actually just omit these values when they don't exist.
                    # We'll know what it means when they're missing on the client side.
                    else:
                        comb_show['episodes'][episode_id] = {
                            k_rating: -1,
                            k_votes: 0
                        }
                    '''
                    comb_show[k_episode_list][episode_id][k_title] = titles[episode_id]['primaryTitle']
                    if episode_obj['season'] != -1:
                        comb_show[k_episode_list][episode_id][k_season] = episode_obj['season']
                    if episode_obj['episode'] != -1:
                        comb_show[k_episode_list][episode_id][k_episode] = episode_obj['episode']
                # Don't merge any TV Shows that don't have even a single vote
                if vote_count > 0:
                    combined[show_id] = comb_show
            else:
                self._logger.info("How did we get here?")
        return combined