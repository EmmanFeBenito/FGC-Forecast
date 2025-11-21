import requests
import pandas as pd
import time
import queue
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class SmartStartGGAPI:
    def __init__(self, api_token):
        self.base_url = "https://api.start.gg/gql/alpha"
        self.api_token = api_token
        self.headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json',
        }
        self.request_queue = queue.Queue()
        self.results = {}
        self.request_count = 0
        self.last_request_time = datetime.now()
        self.rate_limit_lock = threading.Lock()
        
    def add_to_queue(self, query_type, identifier, query, variables=None, priority=1):
        self.request_queue.put({
            'type': query_type,
            'id': identifier,
            'query': query,
            'variables': variables or {},
            'priority': priority,
            'timestamp': datetime.now()
        })
    
    def process_queue(self, max_requests=2000):
        processed = 0
        while not self.request_queue.empty() and processed < max_requests:
            request_data = self.request_queue.get()
            result = self.make_safe_request(request_data['query'], request_data['variables'])
            self.results[request_data['id']] = {
                'type': request_data['type'],
                'data': result,
                'timestamp': datetime.now()
            }
            processed += 1
            print(f"Processed {processed}/{max_requests} - {request_data['type']}")
            time.sleep(0.6)

    def make_safe_request(self, query, variables=None, retries=3):
        for attempt in range(retries):
            try:
                with self.rate_limit_lock:
                    time_since_last = (datetime.now() - self.last_request_time).total_seconds()
                    if time_since_last < 0.6:
                        time.sleep(0.6 - time_since_last)
                    
                    payload = {'query': query, 'variables': variables or {}}
                    response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=30)
                    self.last_request_time = datetime.now()
                    self.request_count += 1
                    
                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        print(f"Rate limited. Waiting {retry_after}s...")
                        time.sleep(retry_after)
                        continue
                    if response.status_code >= 500:
                        print(f"Server error {response.status_code}. Retrying...")
                        time.sleep(8)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'errors' in data:
                        print(f"GraphQL errors: {data['errors']}")
                        return None
                    return data['data']
                    
            except Exception as e:
                print(f"Request failed (attempt {attempt+1}): {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

class TournamentDataCollector:
    def __init__(self, api_token):
        self.api = SmartStartGGAPI(api_token)
        self.target_players = {}
        self.one_year_ago = int((datetime.now() - timedelta(days=365)).timestamp())
        self.target_game_name = None
        self.tournament_cache = {}
        
    def collect_tournament_data(self, target_tournament_slug):
        target_players, target_game = self.get_tournament_players_and_game(target_tournament_slug)
        if not target_players:
            print("No players found in target tournament")
            return None
            
        self.target_players = target_players
        self.target_game_name = target_game
        target_player_ids = list(target_players.keys())
        
        print(f"Found {len(target_player_ids)} target players in {target_game}")
        
        player_tournaments = self.get_all_player_tournaments_parallel(target_players)
        
        if not player_tournaments:
            print("No tournament data found for players")
            return None
            
        all_tournament_ids = self.get_all_tournament_ids(player_tournaments)
        print(f"Getting data for {len(all_tournament_ids)} tournaments...")
        
        all_tournament_details = self.batch_process_tournament_data(all_tournament_ids, player_tournaments)
        
        combined_histories = self.create_combined_tournament_histories(player_tournaments, all_tournament_details)
        
        tournament_csv = self.save_tournament_data(target_tournament_slug, combined_histories)
        print(f"Tournament data saved: {tournament_csv}")
        
        shared_tournaments = self.find_shared_tournaments(player_tournaments)
        print(f"Found {len(shared_tournaments)} tournaments with 2+ target players")
        
        if shared_tournaments:
            shared_tournament_details = self.batch_process_tournament_sets(shared_tournaments)
            head_to_head_data = self.extract_head_to_head_matches(shared_tournament_details, target_players)
            print(f"Found {len(head_to_head_data)} head-to-head matches")
            h2h_csv = self.save_head_to_head_data(target_tournament_slug, head_to_head_data)
            print(f"Head-to-head data saved: {h2h_csv}")
        else:
            head_to_head_data = []
            h2h_csv = None
        
        return {
            'csv_files': {
                'tournament_data': tournament_csv, 
                'head_to_head_data': h2h_csv
            },
            'statistics': {
                'target_players': len(target_player_ids),
                'game': target_game,
                'total_tournaments': len(all_tournament_ids),
                'shared_tournaments': len(shared_tournaments),
                'head_to_head_matches': len(head_to_head_data),
                'api_requests': self.api.request_count
            }
        }
    
    def get_tournament_players_and_game(self, slug):
        query = """
        query TournamentPlayers($slug: String!) {
            tournament(slug: $slug) {
                events {
                    name
                    videogame {
                        name
                    }
                    entrants(query: {perPage: 30}) {
                        nodes {
                            participants {
                                player { 
                                    id 
                                    gamerTag 
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        self.api.add_to_queue('tournament_players', slug, query, {'slug': slug}, 1)
        self.api.process_queue(max_requests=100)
        
        result = self.api.results.get(slug)
        players = {}
        game_name = None
        
        if result and result['data'] and result['data'].get('tournament'):
            events = result['data']['tournament']['events']
            for event in events:
                if not game_name:
                    game_name = event.get('videogame', {}).get('name', 'Unknown Game')
                event_game = event.get('videogame', {}).get('name', 'Unknown')
                if event_game != game_name:
                    continue
                    
                for entrant in event.get('entrants', {}).get('nodes', []):
                    for participant in entrant['participants']:
                        player = participant['player']
                        players[player['id']] = player
        else:
            print(f"Error: Could not fetch tournament data for slug: {slug}")
        
        return players, game_name
    
    def get_all_player_tournaments_parallel(self, players):
        print("Getting tournaments for all players...")
        
        def get_single_player_tournaments(player_id, player_info):
            return self.get_player_tournaments_simple(player_id, player_info)
        
        player_tournaments = {}
        player_list = list(players.items())
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_player = {
                executor.submit(get_single_player_tournaments, pid, info): (pid, info) 
                for pid, info in player_list
            }
            
            for future in as_completed(future_to_player):
                player_id, player_info = future_to_player[future]
                try:
                    tournaments = future.result()
                    if tournaments:
                        player_tournaments[player_id] = {
                            'player': player_info,
                            'tournaments': tournaments
                        }
                except Exception as e:
                    print(f"Error getting tournaments for {player_info['gamerTag']}: {e}")
        
        return player_tournaments
    
    def get_player_tournaments_simple(self, player_id, player_info):
        query = """
        query PlayerTournaments($playerId: ID!, $perPage: Int!) {
            player(id: $playerId) {
                id
                gamerTag
                sets(perPage: $perPage, page: 1) {
                    nodes {
                        id
                        event {
                            id
                            name
                            videogame {
                                name
                            }
                            tournament {
                                id
                                slug
                                name
                                startAt
                            }
                        }
                    }
                    pageInfo {
                        totalPages
                    }
                }
            }
        }
        """
        self.api.add_to_queue('player_sets', player_id, query, {'playerId': player_id, 'perPage': 60}, 2)
        self.api.process_queue(max_requests=1)
        
        result = self.api.results.get(player_id)
        tournaments = {}
        
        if result and result['data']:
            sets_data = result['data']['player']['sets']['nodes']
            total_pages = result['data']['player']['sets']['pageInfo']['totalPages']
            
            for set_data in sets_data:
                event = set_data['event']
                tournament = event['tournament']
                event_game = event.get('videogame', {}).get('name', 'Unknown')
                
                if self.target_game_name and event_game != self.target_game_name:
                    continue
                    
                tournament_date = tournament.get('startAt')
                current_time = int(datetime.now().timestamp())
                
                if tournament_date and tournament_date >= self.one_year_ago and tournament_date <= current_time:
                    tourney_id = tournament['id']
                    tournaments[tourney_id] = {
                        'slug': tournament['slug'],
                        'name': tournament['name'],
                        'date': tournament_date
                    }
            
            if total_pages > 1:
                additional_tournaments = self.get_additional_player_sets_pages(player_id, player_info, total_pages)
                tournaments.update(additional_tournaments)
        
        print(f"  {player_info['gamerTag']}: {len(tournaments)} tournaments")
        return tournaments
    
    def get_additional_player_sets_pages(self, player_id, player_info, total_pages):
        tournaments = {}
        max_additional_pages = min(4, total_pages - 1)
        
        for page in range(2, 2 + max_additional_pages):
            query = """
            query PlayerTournamentsPage($playerId: ID!, $perPage: Int!, $page: Int!) {
                player(id: $playerId) {
                    id
                    sets(perPage: $perPage, page: $page) {
                        nodes {
                            event {
                                videogame {
                                    name
                                }
                                tournament {
                                    id
                                    slug
                                    name
                                    startAt
                                }
                            }
                        }
                    }
                }
            }
            """
            self.api.add_to_queue(f'player_sets_page_{page}', f"{player_id}_page{page}", 
                                query, {'playerId': player_id, 'perPage': 60, 'page': page}, 2)
            self.api.process_queue(max_requests=1)
            
            result = self.api.results.get(f"{player_id}_page{page}")
            if result and result['data']:
                sets_data = result['data']['player']['sets']['nodes']
                for set_data in sets_data:
                    event = set_data['event']
                    tournament = event['tournament']
                    event_game = event.get('videogame', {}).get('name', 'Unknown')
                    
                    if self.target_game_name and event_game != self.target_game_name:
                        continue
                        
                    tournament_date = tournament.get('startAt')
                    current_time = int(datetime.now().timestamp())
                    
                    if tournament_date and tournament_date >= self.one_year_ago and tournament_date <= current_time:
                        tourney_id = tournament['id']
                        tournaments[tourney_id] = {
                            'slug': tournament['slug'],
                            'name': tournament['name'],
                            'date': tournament_date
                        }
        
        return tournaments
    
    def get_all_tournament_ids(self, player_tournaments):
        all_tournament_ids = set()
        for player_data in player_tournaments.values():
            all_tournament_ids.update(player_data['tournaments'].keys())
        return all_tournament_ids
    
    def batch_process_tournament_data(self, tournament_ids, player_tournaments):
        print(f"Processing {len(tournament_ids)} tournaments...")
        
        tournament_details = {}
        tournament_slugs = {}
        for tourney_id in tournament_ids:
            for player_data in player_tournaments.values():
                if tourney_id in player_data['tournaments']:
                    tournament_slugs[tourney_id] = player_data['tournaments'][tourney_id]['slug']
                    break
        
        tournament_list = list(tournament_slugs.items())
        batch_size = 12
        
        for i in range(0, len(tournament_list), batch_size):
            batch = tournament_list[i:i+batch_size]
            print(f"Processing tournaments {i+1}-{min(i+batch_size, len(tournament_list))}/{len(tournament_list)}")
            
            for tourney_id, slug in batch:
                if slug in self.tournament_cache:
                    tournament_details[tourney_id] = self.tournament_cache[slug]
                    continue
                    
                query = """
                query TournamentData($slug: String!) {
                    tournament(slug: $slug) {
                        id
                        name
                        startAt
                        events {
                            id
                            name
                            videogame {
                                name
                            }
                            numEntrants
                            standings(query: {perPage: 50}) {
                                nodes {
                                    placement
                                    entrant {
                                        id
                                        participants {
                                            player {
                                                id
                                                gamerTag
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """
                self.api.add_to_queue('tournament_data', tourney_id, query, {'slug': slug}, 3)
            
            self.api.process_queue(max_requests=len(batch))
            
            for tourney_id, slug in batch:
                if slug in self.tournament_cache:
                    continue
                    
                result = self.api.results.get(tourney_id)
                if result and result['data'] and result['data']['tournament']:
                    tournament_details[tourney_id] = result['data']['tournament']
                    self.tournament_cache[slug] = result['data']['tournament']
        
        return tournament_details
    
    def create_combined_tournament_histories(self, player_tournaments, tournament_details):
        print("Creating tournament histories...")
        
        combined_data = []
        
        for player_id, player_data in player_tournaments.items():
            player_info = player_data['player']
            
            for tourney_id in player_data['tournaments']:
                tournament_info = player_data['tournaments'][tourney_id]
                
                record = {
                    'player_id': player_id,
                    'player_tag': player_info['gamerTag'],
                    'tournament_id': tourney_id,
                    'tournament_name': tournament_info['name'],
                    'tournament_slug': tournament_info['slug'],
                    'tournament_date': datetime.fromtimestamp(tournament_info['date']).strftime('%Y-%m-%d') if tournament_info['date'] else 'Unknown',
                    'placement': 0,
                    'event_name': 'Unknown',
                    'total_entrants': 0
                }
                
                if tourney_id in tournament_details:
                    placement, event_name, total_entrants = self.find_player_data(tournament_details[tourney_id], player_id)
                    
                    if placement:
                        record['placement'] = placement
                        record['event_name'] = event_name
                    
                    if total_entrants:
                        record['total_entrants'] = total_entrants
                
                combined_data.append(record)
        
        return combined_data
    
    def find_player_data(self, tournament_data, player_id):
        placement = 0
        event_name = 'Unknown'
        total_entrants = 0
        
        for event in tournament_data.get('events', []):
            event_game = event.get('videogame', {}).get('name', 'Unknown')
            if event_game != self.target_game_name:
                continue
                
            for standing in event.get('standings', {}).get('nodes', []):
                for participant in standing.get('entrant', {}).get('participants', []):
                    if participant.get('player', {}).get('id') == player_id:
                        placement = standing.get('placement', 0)
                        event_name = event.get('name', 'Unknown')
                        total_entrants = event.get('numEntrants', 0)
                        return placement, event_name, total_entrants
        
        return placement, event_name, total_entrants
    
    def find_shared_tournaments(self, player_tournaments):
        tournament_player_counts = {}
        for player_id, player_data in player_tournaments.items():
            for tourney_id in player_data['tournaments'].keys():
                if tourney_id not in tournament_player_counts:
                    tournament_player_counts[tourney_id] = set()
                tournament_player_counts[tourney_id].add(player_id)
        
        shared_tournaments = {}
        for tourney_id, players_in_tourney in tournament_player_counts.items():
            if len(players_in_tourney) >= 2:
                for player_data in player_tournaments.values():
                    if tourney_id in player_data['tournaments']:
                        tourney_info = player_data['tournaments'][tourney_id]
                        shared_tournaments[tourney_id] = {
                            'slug': tourney_info['slug'],
                            'name': tourney_info['name'],
                            'date': tourney_info['date'],
                            'players': list(players_in_tourney),
                            'player_count': len(players_in_tourney)
                        }
                        break
        
        return shared_tournaments
    
    def batch_process_tournament_sets(self, shared_tournaments):
        print(f"Processing sets for {len(shared_tournaments)} shared tournaments...")
        
        tournament_details = {}
        tournament_list = list(shared_tournaments.items())
        batch_size = 12
        
        for i in range(0, len(tournament_list), batch_size):
            batch = tournament_list[i:i+batch_size]
            print(f"Processing tournaments {i+1}-{min(i+batch_size, len(tournament_list))}/{len(tournament_list)}")
            
            for tourney_id, tourney_info in batch:
                query = """
                query TournamentSets($slug: String!) {
                    tournament(slug: $slug) {
                        id
                        name
                        startAt
                        events {
                            id
                            name
                            videogame {
                                name
                            }
                            sets(perPage: 30) {
                                nodes {
                                    id
                                    slots {
                                        entrant {
                                            id
                                            participants {
                                                player {
                                                    id
                                                    gamerTag
                                                }
                                            }
                                        }
                                        standing {
                                            placement
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """
                self.api.add_to_queue('tournament_sets', tourney_id, query, {'slug': tourney_info['slug']}, 4)
            
            self.api.process_queue(max_requests=len(batch))
            
            for tourney_id, tourney_info in batch:
                result = self.api.results.get(tourney_id)
                if result and result['data'] and result['data']['tournament']:
                    tournament_details[tourney_id] = result['data']['tournament']
        
        return tournament_details
    
    def extract_head_to_head_matches(self, tournament_details, target_players):
        head_to_head_matches = []
        target_player_ids = set(target_players.keys())
        
        for tourney_id, tourney_data in tournament_details.items():
            matches = self.extract_matches_from_tournament(tourney_data, target_player_ids)
            head_to_head_matches.extend(matches)
        
        return head_to_head_matches
    
    def extract_matches_from_tournament(self, tourney_data, target_player_ids):
        matches = []
        
        for event in tourney_data.get('events', []):
            event_game = event.get('videogame', {}).get('name', 'Unknown')
            if event_game != self.target_game_name:
                continue
                
            sets = event.get('sets', {}).get('nodes', [])
            for set_data in sets:
                match = self.analyze_set_for_h2h(set_data, target_player_ids, tourney_data)
                if match:
                    matches.append(match)
        
        return matches
    
    def analyze_set_for_h2h(self, set_data, target_player_ids, tourney_data):
        slots = set_data.get('slots', [])
        if len(slots) != 2:
            return None
        
        players_in_set = set()
        player_details = {}
        
        for slot in slots:
            entrant = slot.get('entrant')
            if not entrant:
                continue
            for participant in entrant.get('participants', []):
                player_data = participant.get('player', {})
                player_id = player_data.get('id')
                if player_id in target_player_ids:
                    players_in_set.add(player_id)
                    standing = slot.get('standing', {}) or {}
                    player_details[player_id] = {
                        'id': player_id,
                        'tag': player_data.get('gamerTag', 'Unknown'),
                        'placement': standing.get('placement') if standing else None
                    }
        
        if len(players_in_set) == 2:
            player_ids = list(players_in_set)
            player1 = player_details[player_ids[0]]
            player2 = player_details[player_ids[1]]
            
            winner_id = None
            if player1['placement'] == 1:
                winner_id = player1['id']
            elif player2['placement'] == 1:
                winner_id = player2['id']
            
            if winner_id:
                winner = player_details[winner_id]
                loser_id = player_ids[0] if player_ids[0] != winner_id else player_ids[1]
                loser = player_details[loser_id]
                
                score = self.get_set_score(set_data['id'])
                
                return {
                    'player1_id': player1['id'],
                    'player1_tag': player1['tag'],
                    'player2_id': player2['id'],
                    'player2_tag': player2['tag'],
                    'winner_id': winner['id'],
                    'winner_tag': winner['tag'],
                    'loser_id': loser['id'],
                    'loser_tag': loser['tag'],
                    'score': score,
                    'set_id': set_data.get('id', 'Unknown'),
                    'tournament_name': tourney_data.get('name', 'Unknown'),
                    'tournament_date': datetime.fromtimestamp(tourney_data.get('startAt', 0)).strftime('%Y-%m-%d') if tourney_data.get('startAt') else 'Unknown'
                }
        
        return None

    def get_set_score(self, set_id):
        query = """
        query SetScore($setId: ID!) {
            set(id: $setId) {
                slots {
                    entrant {
                        id
                    }
                    standing {
                        stats {
                            score {
                                value
                            }
                        }
                    }
                }
            }
        }
        """
        self.api.add_to_queue('set_score', f"score_{set_id}", query, {'setId': set_id}, 5)
        self.api.process_queue(max_requests=1)
        
        result = self.api.results.get(f"score_{set_id}")
        if result and result['data'] and result['data']['set']:
            slots = result['data']['set']['slots']
            if len(slots) == 2:
                score1 = slots[0].get('standing', {}).get('stats', {}).get('score', {}).get('value', 0)
                score2 = slots[1].get('standing', {}).get('stats', {}).get('score', {}).get('value', 0)
                return f"{score1}-{score2}"
        
        return "0-0"

    def save_tournament_data(self, target_slug, combined_histories):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"tournament_data_{target_slug.replace('/', '_')}_{timestamp}.csv"
        
        df = pd.DataFrame(combined_histories)
        if not df.empty:
            df = df.sort_values(['player_tag', 'tournament_date'])
        df.to_csv(filename, index=False)
        return filename
    
    def save_head_to_head_data(self, target_slug, head_to_head_data):
        if not head_to_head_data:
            return None
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"head_to_head_matches_{target_slug.replace('/', '_')}_{timestamp}.csv"
        
        h2h_data = []
        for match in head_to_head_data:
            h2h_data.append({
                'player1_id': match['player1_id'],
                'player1_tag': match['player1_tag'],
                'player2_id': match['player2_id'],
                'player2_tag': match['player2_tag'],
                'winner_id': match['winner_id'],
                'winner_tag': match['winner_tag'],
                'loser_id': match['loser_id'],
                'loser_tag': match['loser_tag'],
                'score': f"'{match['score']}",  # Add apostrophe to prevent Excel date conversion
                'set_id': match['set_id'],
                'tournament_name': match['tournament_name'],
                'tournament_date': match['tournament_date']
            })
        
        df = pd.DataFrame(h2h_data)
        if not df.empty:
            df = df.sort_values(['tournament_date', 'player1_tag'])
        df.to_csv(filename, index=False)
        return filename

def main():
    API_TOKEN = "eedc3bb80f49c2e1e45d32c2bb649336"
    
    if API_TOKEN == "YOUR_TOKEN_HERE":
        print("Please set your API token!")
        return
    
    collector = TournamentDataCollector(API_TOKEN)
    
    tournament_slug = input("Enter tournament slug: ").strip()
    if not tournament_slug:
        print("No tournament provided")
        return
    
    start_time = time.time()
    data = collector.collect_tournament_data(tournament_slug)
    end_time = time.time()
    
    if data:
        print(f"Collection complete in {end_time - start_time:.2f} seconds!")
        print(f"Target players: {data['statistics']['target_players']}")
        print(f"Total tournaments: {data['statistics']['total_tournaments']}")
        print(f"Shared tournaments: {data['statistics']['shared_tournaments']}")
        print(f"Head-to-head matches: {data['statistics']['head_to_head_matches']}")
        print(f"API requests: {data['statistics']['api_requests']}")
        print(f"CSV files: {data['csv_files']}")

if __name__ == "__main__":
    main()
