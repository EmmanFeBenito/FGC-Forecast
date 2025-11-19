import requests
import pandas as pd
import time
import queue
from datetime import datetime, timedelta

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
            result = self.make_safe_request(
                request_data['query'], 
                request_data['variables']
            )
            
            self.results[request_data['id']] = {
                'type': request_data['type'],
                'data': result,
                'timestamp': datetime.now()
            }
            
            processed += 1
            print(f"Processed {processed}/{max_requests} - {request_data['type']}")
            
            time.sleep(2)
    
    def make_safe_request(self, query, variables=None, retries=3):
        for attempt in range(retries):
            try:
                time_since_last = (datetime.now() - self.last_request_time).total_seconds()
                if time_since_last < 2:
                    time.sleep(2 - time_since_last)
                
                payload = {'query': query, 'variables': variables or {}}
                response = requests.post(self.base_url, headers=self.headers, 
                                       json=payload, timeout=30)
                
                self.last_request_time = datetime.now()
                self.request_count += 1
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                    
                if response.status_code >= 500:
                    print(f"Server error {response.status_code}. Retrying...")
                    time.sleep(10)
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
                    time.sleep(10 * (attempt + 1))
        
        return None

class TournamentDataCollector:
    def __init__(self, api_token):
        self.api = SmartStartGGAPI(api_token)
        self.target_players = {}
        self.one_year_ago = int((datetime.now() - timedelta(days=365)).timestamp())
        self.target_game_name = None
        
    def collect_tournament_data(self, target_tournament_slug):
        target_players, target_game = self.get_tournament_players_and_game(target_tournament_slug)
        if not target_players:
            print("No players found in target tournament")
            return None
            
        self.target_players = target_players
        self.target_game_name = target_game
        target_player_ids = list(target_players.keys())
        print(f"Found {len(target_player_ids)} target players in {target_game}")
        
        player_tournaments = self.get_player_tournaments(target_players)
        shared_tournaments = self.find_shared_tournaments(player_tournaments)
        print(f"Found {len(shared_tournaments)} tournaments with 2+ target players")
        
        head_to_head_data = self.get_head_to_head_matches(shared_tournaments, target_players)
        player_histories = self.get_tournament_histories(player_tournaments, target_players)
        csv_files = self.save_to_csv(target_tournament_slug, player_histories, head_to_head_data)
        
        return {
            'csv_files': csv_files,
            'statistics': {
                'target_players': len(target_player_ids),
                'game': target_game,
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
                    entrants(query: {perPage: 100}) {
                        nodes {
                            participants {
                                player { 
                                    id 
                                    gamerTag 
                                }
                            }
                        }
                    }
                    standings(query: {perPage: 100}) {
                        nodes {
                            entrant {
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
        self.api.add_to_queue('tournament_players', slug, query, {'slug': slug}, 1)
        self.api.process_queue(max_requests=100)
        
        result = self.api.results.get(slug)
        players = {}
        game_name = None
        
        if result and result['data']:
            events = result['data']['tournament']['events']
            
            for event in events:
                if not game_name:
                    game_name = event.get('videogame', {}).get('name', 'Unknown Game')
                    print(f"Target game: {game_name}")
                
                event_game = event.get('videogame', {}).get('name', 'Unknown')
                if event_game != game_name:
                    continue
                    
                for entrant in event.get('entrants', {}).get('nodes', []):
                    for participant in entrant['participants']:
                        player = participant['player']
                        players[player['id']] = player
                
                for standing in event.get('standings', {}).get('nodes', []):
                    for participant in standing['entrant']['participants']:
                        player = participant['player']
                        players[player['id']] = player
        
        return players, game_name
    
    def get_player_tournaments(self, players):
        print("Getting tournaments for each player...")
        player_tournaments = {}
        
        for player_id, player_info in players.items():
            print(f"Getting tournaments for {player_info['gamerTag']}...")
            
            query = """
            query PlayerTournaments($playerId: ID!, $perPage: Int!) {
                player(id: $playerId) {
                    id
                    gamerTag
                    sets(perPage: $perPage) {
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
            self.api.add_to_queue('player_tournaments', player_id, query, 
                                {'playerId': player_id, 'perPage': 50}, 2)
        
        self.api.process_queue(max_requests=len(players) * 2)
        
        for player_id, player_info in players.items():
            result = self.api.results.get(player_id)
            if result and result['data']:
                tournaments = {}
                sets_data = result['data']['player']['sets']['nodes']
                
                for set_data in sets_data:
                    event = set_data['event']
                    tournament = event['tournament']
                    
                    event_game = event.get('videogame', {}).get('name', 'Unknown')
                    if event_game != self.target_game_name:
                        continue
                    
                    tourney_id = tournament['id']
                    tournament_date = tournament.get('startAt')
                    
                    if tournament_date and tournament_date >= self.one_year_ago:
                        if tourney_id not in tournaments:
                            tournaments[tourney_id] = {
                                'slug': tournament['slug'],
                                'name': tournament['name'],
                                'date': tournament_date
                            }
                
                player_tournaments[player_id] = {
                    'player': player_info,
                    'tournaments': tournaments
                }
                print(f"  {player_info['gamerTag']}: {len(tournaments)} tournaments")
        
        return player_tournaments
    
    def find_shared_tournaments(self, player_tournaments):
        print("Finding tournaments with multiple target players...")
        
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
                            'players': list(players_in_tourney)
                        }
                        break
        
        return shared_tournaments
    
    def get_head_to_head_matches(self, shared_tournaments, target_players):
        print("Getting head-to-head matches from shared tournaments...")
        head_to_head_matches = []
        
        for tourney_id, tourney_info in shared_tournaments.items():
            print(f"Checking {tourney_info['name']} for head-to-head matches...")
            
            query = """
            query TournamentSets($slug: String!, $perPage: Int!) {
                tournament(slug: $slug) {
                    events {
                        name
                        videogame {
                            name
                        }
                        sets(perPage: $perPage) {
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
                                }
                            }
                        }
                    }
                }
            }
            """
            self.api.add_to_queue('tournament_sets', tourney_id, query, 
                                {'slug': tourney_info['slug'], 'perPage': 100}, 3)
        
        self.api.process_queue(max_requests=len(shared_tournaments))
        
        for tourney_id, tourney_info in shared_tournaments.items():
            result = self.api.results.get(tourney_id)
            if result and result['data']:
                target_player_ids = set(tourney_info['players'])
                
                for event in result['data']['tournament']['events']:
                    event_game = event.get('videogame', {}).get('name', 'Unknown')
                    if event_game != self.target_game_name:
                        continue
                    
                    event_name = event.get('name', 'Unknown')
                    sets_data = event.get('sets', {}).get('nodes', [])
                    
                    for set_data in sets_data:
                        slots = set_data.get('slots', [])
                        if len(slots) != 2:
                            continue
                        
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
                                    player_details[player_id] = {
                                        'id': player_id,
                                        'tag': player_data.get('gamerTag', 'Unknown')
                                    }
                        
                        if len(players_in_set) == 2:
                            set_id = set_data['id']
                            set_detail_query = """
                            query SetDetail($setId: ID!) {
                                set(id: $setId) {
                                    id
                                    slots {
                                        standing {
                                            placement
                                            stats {
                                                score {
                                                    value
                                                }
                                            }
                                        }
                                        entrant {
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
                            """
                            self.api.add_to_queue('set_detail', set_id, set_detail_query, {'setId': set_id}, 4)
        
        self.api.process_queue(max_requests=200)
        
        for tourney_id, tourney_info in shared_tournaments.items():
            result = self.api.results.get(tourney_id)
            if result and result['data']:
                target_player_ids = set(tourney_info['players'])
                
                for event in result['data']['tournament']['events']:
                    event_game = event.get('videogame', {}).get('name', 'Unknown')
                    if event_game != self.target_game_name:
                        continue
                    
                    event_name = event.get('name', 'Unknown')
                    sets_data = event.get('sets', {}).get('nodes', [])
                    
                    for set_data in sets_data:
                        set_id = set_data['id']
                        set_detail_result = self.api.results.get(set_id)
                        
                        if set_detail_result and set_detail_result['data']:
                            set_detail = set_detail_result['data']['set']
                            slots = set_detail.get('slots', [])
                            
                            if len(slots) == 2:
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
                                            player_details[player_id] = {
                                                'id': player_id,
                                                'tag': player_data.get('gamerTag', 'Unknown')
                                            }
                                
                                if len(players_in_set) == 2:
                                    player_ids = list(players_in_set)
                                    player1 = player_details[player_ids[0]]
                                    player2 = player_details[player_ids[1]]
                                    
                                    winner_slot = None
                                    loser_slot = None
                                    
                                    for slot in slots:
                                        placement = slot.get('standing', {}).get('placement')
                                        if placement == 1:
                                            winner_slot = slot
                                        elif placement == 2:
                                            loser_slot = slot
                                    
                                    if winner_slot and loser_slot:
                                        winner_id = None
                                        winner_entrant = winner_slot.get('entrant')
                                        if winner_entrant:
                                            for participant in winner_entrant.get('participants', []):
                                                player_id = participant.get('player', {}).get('id')
                                                if player_id in players_in_set:
                                                    winner_id = player_id
                                                    break
                                        
                                        if winner_id:
                                            winner = player_details[winner_id]
                                            loser = player_details[player_ids[0] if player_ids[0] != winner_id else player_ids[1]]
                                            
                                            winner_score = winner_slot.get('standing', {}).get('stats', {}).get('score', {}).get('value', 'Unknown')
                                            loser_score = loser_slot.get('standing', {}).get('stats', {}).get('score', {}).get('value', 'Unknown')
                                            
                                            score_display = f"Score: {winner_score}-{loser_score}"
                                            
                                            head_to_head_matches.append({
                                                'player1_id': player1['id'],
                                                'player1_tag': player1['tag'],
                                                'player2_id': player2['id'],
                                                'player2_tag': player2['tag'],
                                                'winner_id': winner['id'],
                                                'winner_tag': winner['tag'],
                                                'loser_id': loser['id'],
                                                'loser_tag': loser['tag'],
                                                'winner_score': winner_score,
                                                'loser_score': loser_score,
                                                'match_score': score_display,
                                                'set_id': set_id,
                                                'tournament_name': tourney_info['name'],
                                                'event_name': event_name,
                                                'tournament_date': datetime.fromtimestamp(tourney_info.get('date', 0)).strftime('%Y-%m-%d') if tourney_info.get('date') else 'Unknown'
                                            })
        
        print(f"Found {len(head_to_head_matches)} head-to-head matches")
        return head_to_head_matches
    
    def get_tournament_histories(self, player_tournaments, target_players):
        print("Getting detailed tournament histories...")
        
        all_tournaments = {}
        for player_data in player_tournaments.values():
            all_tournaments.update(player_data['tournaments'])
        
        print(f"Getting detailed data for {len(all_tournaments)} tournaments...")
        
        for tourney_id, tourney_info in all_tournaments.items():
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
                        standings(query: {perPage: 100}) {
                            nodes {
                                placement
                                entrant {
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
            self.api.add_to_queue('tournament_data', tourney_id, query, 
                                {'slug': tourney_info['slug']}, 5)
        
        self.api.process_queue(max_requests=len(all_tournaments) * 2)
        
        tournament_details = {}
        for tourney_id in all_tournaments.keys():
            result = self.api.results.get(tourney_id)
            if result and result['data'] and result['data']['tournament']:
                tournament_details[tourney_id] = result['data']['tournament']
        
        print(f"Successfully retrieved {len(tournament_details)} tournament details")
        
        player_histories = []
        players_with_no_history = []
        
        for player_id, player_data in player_tournaments.items():
            player_info = player_data['player']
            tournaments = player_data['tournaments']
            
            if not tournaments:
                players_with_no_history.append(player_info)
                continue
            
            for tourney_id in tournaments.keys():
                if tourney_id in tournament_details:
                    tourney_data = tournament_details[tourney_id]
                    
                    placement, event_name = self.find_player_placement(tourney_data, player_id)
                    total_entrants = self.find_total_entrants(tourney_data, event_name)
                    
                    if placement:
                        player_histories.append({
                            'player_id': player_id,
                            'player_tag': player_info['gamerTag'],
                            'tournament_name': tourney_data.get('name', 'Unknown'),
                            'tournament_date': datetime.fromtimestamp(tourney_data.get('startAt', 0)).strftime('%Y-%m-%d') if tourney_data.get('startAt') else 'Unknown',
                            'event_name': event_name,
                            'placement': placement,
                            'total_entrants': total_entrants
                        })
        
        for player_info in players_with_no_history:
            player_histories.append({
                'player_id': player_info['id'],
                'player_tag': player_info['gamerTag'],
                'tournament_name': 'N/A',
                'tournament_date': 'N/A',
                'event_name': 'N/A',
                'placement': 'N/A',
                'total_entrants': 'N/A'
            })
        
        print(f"Created tournament histories for {len(player_histories)} player-tournament combinations")
        return player_histories
    
    def find_player_placement(self, tournament_data, player_id):
        for event in tournament_data.get('events', []):
            event_game = event.get('videogame', {}).get('name', 'Unknown')
            if event_game != self.target_game_name:
                continue
                
            for standing in event.get('standings', {}).get('nodes', []):
                for participant in standing.get('entrant', {}).get('participants', []):
                    if participant.get('player', {}).get('id') == player_id:
                        return standing.get('placement'), event.get('name', 'Unknown')
        return None, None
    
    def find_total_entrants(self, tournament_data, event_name):
        for event in tournament_data.get('events', []):
            if event.get('name') == event_name:
                return event.get('numEntrants', 0)
        return 0
    
    def save_to_csv(self, target_slug, player_histories, head_to_head_data):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"tournament_data_{target_slug.replace('/', '_')}_{timestamp}"
        
        csv_files = {}
        
        history_file = f"{base_filename}_player_histories.csv"
        df_history = pd.DataFrame(player_histories)
        if not df_history.empty:
            df_history = df_history.sort_values('player_tag')
        df_history.to_csv(history_file, index=False)
        csv_files['player_histories'] = history_file
        print(f"Saved player histories: {history_file}")
        
        if head_to_head_data:
            matches_file = f"{base_filename}_head_to_head_matches.csv"
            df_matches = pd.DataFrame(head_to_head_data)
            if not df_matches.empty:
                df_matches = df_matches.sort_values('player1_tag')
            df_matches.to_csv(matches_file, index=False)
            csv_files['head_to_head_matches'] = matches_file
            print(f"Saved head-to-head matches: {matches_file}")
        else:
            csv_files['head_to_head_matches'] = None
            print("No head-to-head matches found")
        
        players_with_history = len(set([h['player_id'] for h in player_histories if h['tournament_name'] != 'N/A']))
        players_without_history = len([h for h in player_histories if h['tournament_name'] == 'N/A'])
        print(f"Summary: {players_with_history} players with history, {players_without_history} players without history")
        
        return csv_files

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
    
    data = collector.collect_tournament_data(tournament_slug)
    
    if data:
        print(f"Collection complete!")
        print(f"Statistics:")
        print(f"   Target game: {data['statistics']['game']}")
        print(f"   Target players: {data['statistics']['target_players']}")
        print(f"   Shared tournaments: {data['statistics']['shared_tournaments']}")
        print(f"   Head-to-head matches: {data['statistics']['head_to_head_matches']}")
        print(f"   API requests: {data['statistics']['api_requests']}")
        print(f"   CSV files: {data['csv_files']}")
    else:
        print("Data collection failed")

if __name__ == "__main__":
    main()
