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
        self.target_players = set()
        self.one_year_ago = int((datetime.now() - timedelta(days=365)).timestamp())
        self.target_game_name = None
        
    def collect_tournament_data(self, target_tournament_slug):
        target_players, target_game = self.get_tournament_players_and_game(target_tournament_slug)
        if not target_players:
            print("No players found in target tournament")
            return None
            
        self.target_players = set(target_players.keys())
        self.target_game_name = target_game
        print(f"Found {len(self.target_players)} target players in {target_game}: {[p['gamerTag'] for p in target_players.values()]}")
        
        player_histories = self.get_player_tournament_histories(target_players)
        
        csv_file = self.save_to_csv(target_tournament_slug, player_histories, target_players)
        
        return {
            'csv_file': csv_file,
            'statistics': {
                'target_players': len(self.target_players),
                'game': target_game,
                'api_requests': self.api.request_count
            }
        }
    
    def get_tournament_players_and_game(self, slug):
        query = """
        query TournamentPlayers($slug: String!) {
            tournament(slug: $slug) {
                name
                events {
                    id
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
            tournament = result['data']['tournament']
            events = tournament['events']
            
            if events:
                first_event = events[0]
                game_name = first_event.get('videogame', {}).get('name', 'Unknown Game')
                print(f"Target game: {game_name}")
            
            for event in events:
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
    
    def get_player_tournament_histories(self, players):
        print("Getting player tournament histories...")
        
        for player_id, player_info in players.items():
            query = """
            query PlayerHistory($playerId: ID!, $perPage: Int!) {
                player(id: $playerId) {
                    id
                    gamerTag
                    sets(perPage: $perPage) {
                        nodes {
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
                    }
                }
            }
            """
            self.api.add_to_queue('player_history', player_id, query, 
                                {'playerId': player_id, 'perPage': 100}, 2)
        
        self.api.process_queue(max_requests=len(players) * 2)
        
        player_histories = {}
        players_with_no_history = []
        
        for player_id, player_info in players.items():
            result = self.api.results.get(player_id)
            if result and result['data']:
                tournaments = self.extract_tournaments(result['data'])
                player_histories[player_id] = {
                    'player': player_info,
                    'tournaments': tournaments
                }
                print(f"{player_info['gamerTag']}: {len(tournaments)} {self.target_game_name} tournaments")
            else:
                players_with_no_history.append(player_info)
                print(f"{player_info['gamerTag']}: No tournament history found")
        
        detailed_histories = self.get_detailed_tournament_data(player_histories)
        
        for player_info in players_with_no_history:
            detailed_histories.append({
                'player_id': player_info['id'],
                'player_tag': player_info['gamerTag'],
                'tournament_name': 'N/A',
                'tournament_date': 'N/A',
                'event_name': 'N/A',
                'placement': 'N/A',
                'total_entrants': 'N/A'
            })
        
        return detailed_histories
    
    def extract_tournaments(self, player_data):
        tournaments = {}
        sets = player_data['player']['sets']['nodes']
        
        for set_data in sets:
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
        
        return tournaments
    
    def get_detailed_tournament_data(self, player_histories):
        print("Getting detailed tournament data...")
        
        all_tournaments = {}
        for player_data in player_histories.values():
            all_tournaments.update(player_data['tournaments'])
        
        print(f"Getting data for {len(all_tournaments)} tournaments...")
        
        tournament_details = {}
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
                                {'slug': tourney_info['slug']}, 3)
        
        self.api.process_queue(max_requests=len(all_tournaments) * 2)
        
        for tourney_id in all_tournaments.keys():
            result = self.api.results.get(tourney_id)
            if result and result['data']:
                tournament_details[tourney_id] = result['data']['tournament']
        
        detailed_histories = []
        for player_id, player_data in player_histories.items():
            player_info = player_data['player']
            player_tournaments = player_data['tournaments']
            
            if not player_tournaments:
                continue
                
            for tourney_id in player_tournaments.keys():
                if tourney_id in tournament_details:
                    tourney_data = tournament_details[tourney_id]
                    
                    placement, event_name = self.find_player_placement(tourney_data, player_id)
                    total_entrants = self.find_total_entrants(tourney_data, event_name)
                    
                    if placement:
                        detailed_histories.append({
                            'player_id': player_id,
                            'player_tag': player_info['gamerTag'],
                            'tournament_name': tourney_data.get('name', 'Unknown'),
                            'tournament_date': datetime.fromtimestamp(tourney_data.get('startAt', 0)).strftime('%Y-%m-%d') if tourney_data.get('startAt') else 'Unknown',
                            'event_name': event_name,
                            'placement': placement,
                            'total_entrants': total_entrants
                        })
        
        return detailed_histories
    
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
    
    def save_to_csv(self, target_slug, player_histories, target_players):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"player_histories_{target_slug.replace('/', '_')}_{timestamp}.csv"
        
        df = pd.DataFrame(player_histories)
        if not df.empty:
            df = df.sort_values('player_tag')
        df.to_csv(filename, index=False)
        
        print(f"Saved player histories: {filename}")
        
        players_with_history = len(set([h['player_id'] for h in player_histories if h['tournament_name'] != 'N/A']))
        players_without_history = len([h for h in player_histories if h['tournament_name'] == 'N/A'])
        print(f"Summary: {players_with_history} players with history, {players_without_history} players without history")
        
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
    
    data = collector.collect_tournament_data(tournament_slug)
    
    if data:
        print(f"Collection complete!")
        print(f"Statistics:")
        print(f"   Target game: {data['statistics']['game']}")
        print(f"   Target players: {data['statistics']['target_players']}")
        print(f"   API requests: {data['statistics']['api_requests']}")
        print(f"   CSV file: {data['csv_file']}")
    else:
        print("Data collection failed")

if __name__ == "__main__":
    main()