"""
NHL Injury Tracker

Fetches and manages injury data from Daily Faceoff to improve prediction accuracy.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import sqlite3
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import urllib.request
import urllib.error


class InjuryTracker:
    """
    Track NHL player injuries to exclude from predictions or adjust expectations.

    Injury statuses:
    - OUT: Player is definitely not playing
    - IR: Injured Reserve (extended absence)
    - LTIR: Long-Term Injured Reserve
    - DTD: Day-to-Day
    - SUSPENDED: Player is suspended
    - QUESTIONABLE: May or may not play
    """

    # Team name to abbreviation mapping for Daily Faceoff URLs
    TEAM_URL_MAP = {
        'ANA': 'anaheim-ducks',
        'ARI': 'utah-hockey-club',  # Formerly Arizona
        'BOS': 'boston-bruins',
        'BUF': 'buffalo-sabres',
        'CAR': 'carolina-hurricanes',
        'CBJ': 'columbus-blue-jackets',
        'CGY': 'calgary-flames',
        'CHI': 'chicago-blackhawks',
        'COL': 'colorado-avalanche',
        'DAL': 'dallas-stars',
        'DET': 'detroit-red-wings',
        'EDM': 'edmonton-oilers',
        'FLA': 'florida-panthers',
        'LAK': 'los-angeles-kings',
        'MIN': 'minnesota-wild',
        'MTL': 'montreal-canadiens',
        'NJD': 'new-jersey-devils',
        'NSH': 'nashville-predators',
        'NYI': 'new-york-islanders',
        'NYR': 'new-york-rangers',
        'OTT': 'ottawa-senators',
        'PHI': 'philadelphia-flyers',
        'PIT': 'pittsburgh-penguins',
        'SEA': 'seattle-kraken',
        'SJS': 'san-jose-sharks',
        'STL': 'st-louis-blues',
        'TBL': 'tampa-bay-lightning',
        'TOR': 'toronto-maple-leafs',
        'UTA': 'utah-hockey-club',
        'VAN': 'vancouver-canucks',
        'VGK': 'vegas-golden-knights',
        'WPG': 'winnipeg-jets',
        'WSH': 'washington-capitals',
    }

    INJURY_WEIGHTS = {
        'OUT': 0.0,
        'IR': 0.0,
        'LTIR': 0.0,
        'SUSPENDED': 0.0,
        'DTD': 0.5,
        'QUESTIONABLE': 0.5,
        'PROBABLE': 0.9,
    }

    def __init__(self, db_path: str = "nhl_stats.db", cache_file: str = "injuries_cache.json",
                 auto_fetch: bool = True):
        self.db_path = db_path
        self.cache_file = Path(cache_file)
        self.injuries: Dict[str, Dict] = {}  # player_name_normalized -> injury info
        cache_loaded = self._load_cache()

        # Auto-fetch if cache is stale or empty and auto_fetch is enabled
        if auto_fetch and not cache_loaded:
            try:
                print("Cache stale or empty, fetching fresh injury data...")
                self.fetch_all_injuries()
            except Exception as e:
                print(f"Warning: Could not fetch injury data: {e}")

    def _load_cache(self) -> bool:
        """Load cached injury data if recent (less than 4 hours old).

        Returns:
            True if cache was loaded successfully, False if cache is stale/missing.
        """
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    cache_time = datetime.fromisoformat(data.get('updated_at', '2000-01-01'))
                    if datetime.now() - cache_time < timedelta(hours=4):
                        self.injuries = data.get('injuries', {})
                        print(f"Loaded {len(self.injuries)} injuries from cache ({cache_time.strftime('%H:%M')})")
                        return True
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Cache load error: {e}")
        return False

    def _save_cache(self):
        """Save injury data to cache file."""
        data = {
            'updated_at': datetime.now().isoformat(),
            'injuries': self.injuries
        }
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _normalize_name(self, name: str) -> str:
        """Normalize player name for consistent matching."""
        name = name.lower().strip()
        name = re.sub(r'\s+', ' ', name)
        # Remove common suffixes/prefixes
        name = name.replace('.', '').replace("'", "").replace("-", " ")
        # Handle special characters
        replacements = {
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
            'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
            'ý': 'y', 'ÿ': 'y',
            'ñ': 'n', 'ç': 'c',
            'ø': 'o', 'å': 'a', 'æ': 'ae',
            'š': 's', 'č': 'c', 'ž': 'z', 'ř': 'r',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name

    def fetch_team_injuries_from_dailyfaceoff(self, team_abbrev: str) -> List[Dict]:
        """
        Fetch injuries for a specific team from Daily Faceoff.
        Returns list of injury dictionaries.
        """
        team_slug = self.TEAM_URL_MAP.get(team_abbrev.upper())
        if not team_slug:
            print(f"Unknown team abbreviation: {team_abbrev}")
            return []

        url = f"https://www.dailyfaceoff.com/teams/{team_slug}/line-combinations/"

        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })

            with urllib.request.urlopen(req, timeout=15) as response:
                html = response.read().decode('utf-8')

            injuries = self._parse_dailyfaceoff_injuries(html, team_abbrev)
            return injuries

        except urllib.error.URLError as e:
            print(f"Failed to fetch {team_abbrev} injuries: {e}")
            return []
        except Exception as e:
            print(f"Error parsing {team_abbrev} injuries: {e}")
            return []

    def _parse_dailyfaceoff_injuries(self, html: str, team_abbrev: str) -> List[Dict]:
        """
        Parse injury information from Daily Faceoff HTML.
        Extracts player data from the embedded __NEXT_DATA__ JSON.
        """
        injuries = []
        seen_players = set()  # Avoid duplicates (players appear in multiple line combos)

        # Extract embedded JSON data from __NEXT_DATA__ script tag
        json_pattern = r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>'
        match = re.search(json_pattern, html)

        if match:
            try:
                import json
                data = json.loads(match.group(1))
                props = data.get('props', {}).get('pageProps', {})
                combinations = props.get('combinations', {})
                players = combinations.get('players', [])

                for player in players:
                    status = player.get('injuryStatus')
                    if status:
                        name = player.get('name', '')
                        if name and name not in seen_players:
                            seen_players.add(name)

                            # Map status to our standard format
                            status_upper = status.upper()
                            if status_upper in ('IR', 'LTIR'):
                                mapped_status = status_upper
                            elif status_upper in ('DTD', 'DAY-TO-DAY'):
                                mapped_status = 'DTD'
                            elif status_upper == 'OUT':
                                mapped_status = 'OUT'
                            else:
                                mapped_status = 'OUT'  # Default unknown to OUT

                            # Extract injury details from latestNews if available
                            injury_type = 'Unknown'
                            news = player.get('latestNews', {})
                            if news and isinstance(news, dict):
                                details = news.get('details', '')
                                if details:
                                    # Try to extract injury type from details
                                    injury_type = self._extract_injury_type(details)

                            injuries.append({
                                'player_name': name,
                                'team': team_abbrev.upper(),
                                'status': mapped_status,
                                'injury_type': injury_type,
                            })

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Error parsing JSON data for {team_abbrev}: {e}")
                # Fall back to regex parsing if JSON fails
                injuries = self._parse_dailyfaceoff_injuries_regex(html, team_abbrev)

        else:
            # Fall back to regex parsing if no JSON found
            injuries = self._parse_dailyfaceoff_injuries_regex(html, team_abbrev)

        return injuries

    def _extract_injury_type(self, details: str) -> str:
        """Extract injury type from news details text."""
        details_lower = details.lower()

        # Common injury patterns
        injury_patterns = [
            (r'lower[- ]body', 'Lower Body'),
            (r'upper[- ]body', 'Upper Body'),
            (r'undisclosed', 'Undisclosed'),
            (r'concussion', 'Concussion'),
            (r'knee', 'Knee'),
            (r'ankle', 'Ankle'),
            (r'shoulder', 'Shoulder'),
            (r'wrist', 'Wrist'),
            (r'back', 'Back'),
            (r'groin', 'Groin'),
            (r'hip', 'Hip'),
            (r'hand', 'Hand'),
            (r'foot', 'Foot'),
            (r'illness', 'Illness'),
        ]

        for pattern, injury_type in injury_patterns:
            if re.search(pattern, details_lower):
                return injury_type

        return 'Unknown'

    def _parse_dailyfaceoff_injuries_regex(self, html: str, team_abbrev: str) -> List[Dict]:
        """
        Fallback regex-based parser for Daily Faceoff HTML.
        Used when JSON parsing fails.
        """
        injuries = []
        seen_players = set()

        # Look for IR (Injured Reserve) mentions
        ir_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*(?:</[^>]+>)*\s*(?:<[^>]+>)*\s*IR\b'
        ir_matches = re.findall(ir_pattern, html, re.IGNORECASE)

        for name in ir_matches:
            name = name.strip()
            # Filter out garbage: must be reasonable name length, have at least 2 words
            if len(name) > 3 and len(name) < 40 and ' ' in name and name not in seen_players:
                # Check if it looks like a real name (not a sentence)
                words = name.split()
                if len(words) <= 4 and all(w[0].isupper() for w in words):
                    seen_players.add(name)
                    injuries.append({
                        'player_name': name,
                        'team': team_abbrev.upper(),
                        'status': 'IR',
                        'injury_type': 'Unknown',
                    })

        # Look for Day-to-Day mentions
        dtd_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*(?:</[^>]+>)*\s*(?:<[^>]+>)*\s*(?:DTD|Day-to-Day)'
        dtd_matches = re.findall(dtd_pattern, html, re.IGNORECASE)

        for name in dtd_matches:
            name = name.strip()
            if len(name) > 3 and len(name) < 40 and ' ' in name and name not in seen_players:
                words = name.split()
                if len(words) <= 4 and all(w[0].isupper() for w in words):
                    seen_players.add(name)
                    injuries.append({
                        'player_name': name,
                        'team': team_abbrev.upper(),
                        'status': 'DTD',
                        'injury_type': 'Unknown',
                    })

        return injuries

    def fetch_all_injuries(self, teams: List[str] = None) -> int:
        """
        Fetch injuries for all teams (or specified teams).
        Returns number of injuries found.
        """
        if teams is None:
            teams = list(self.TEAM_URL_MAP.keys())

        print(f"Fetching injuries for {len(teams)} teams...")
        total_injuries = 0

        for team in teams:
            injuries = self.fetch_team_injuries_from_dailyfaceoff(team)
            for injury in injuries:
                self.add_injury(
                    injury['player_name'],
                    injury['team'],
                    injury['status'],
                    injury.get('injury_type', '')
                )
                total_injuries += 1

        self._save_cache()
        print(f"Found {total_injuries} total injuries across {len(teams)} teams")
        return total_injuries

    def add_injury(self, player_name: str, team: str, status: str,
                   injury_type: str = "", expected_return: str = ""):
        """Add or update an injury in the tracker."""
        status = status.upper()
        if status not in self.INJURY_WEIGHTS:
            status = 'OUT'  # Default to OUT for unknown statuses

        key = self._normalize_name(player_name)

        self.injuries[key] = {
            'player_name': player_name,
            'team': team.upper(),
            'status': status,
            'injury_type': injury_type,
            'expected_return': expected_return,
            'updated_at': datetime.now().isoformat(),
            'weight': self.INJURY_WEIGHTS.get(status, 0.0)
        }

    def add_injury_manual(self, player_name: str, team: str, status: str,
                          injury_type: str = "", expected_return: str = ""):
        """
        Manually add or update an injury (for command line use).
        """
        self.add_injury(player_name, team, status, injury_type, expected_return)
        self._save_cache()
        print(f"Added: {player_name} ({team}) - {status}" +
              (f" [{injury_type}]" if injury_type else ""))

    def remove_injury(self, player_name: str):
        """Remove a player from the injury list."""
        key = self._normalize_name(player_name)
        if key in self.injuries:
            removed = self.injuries.pop(key)
            self._save_cache()
            print(f"Removed {removed['player_name']} from injury list")
        else:
            print(f"'{player_name}' not found in injury list")

    def get_player_status(self, player_name: str) -> Optional[Dict]:
        """Get injury status for a player."""
        key = self._normalize_name(player_name)
        return self.injuries.get(key)

    def is_player_available(self, player_name: str) -> Tuple[bool, float]:
        """
        Check if a player is available to play.

        Returns:
            Tuple of (is_likely_available, probability_weight)
            - (True, 1.0) for healthy players
            - (False, 0.0) for OUT/IR/LTIR/SUSPENDED
            - (True, 0.5) for DTD/QUESTIONABLE
        """
        injury = self.get_player_status(player_name)

        if not injury:
            return (True, 1.0)

        weight = injury.get('weight', 0.0)
        return (weight > 0, weight)

    def get_team_injuries(self, team_abbrev: str) -> List[Dict]:
        """Get all injuries for a specific team."""
        team_abbrev = team_abbrev.upper()
        return [
            injury for injury in self.injuries.values()
            if injury.get('team') == team_abbrev
        ]

    def get_all_out_players(self) -> List[str]:
        """Get list of all players who are definitely OUT."""
        return [
            injury['player_name']
            for injury in self.injuries.values()
            if injury.get('status') in ('OUT', 'IR', 'LTIR', 'SUSPENDED')
        ]

    def get_questionable_players(self) -> List[Dict]:
        """Get list of all players with uncertain status (DTD, QUESTIONABLE)."""
        return [
            injury for injury in self.injuries.values()
            if injury.get('status') in ('DTD', 'QUESTIONABLE')
        ]

    def print_injury_report(self, team_abbrev: str = None):
        """Print formatted injury report."""
        print("\n" + "=" * 60)
        print("NHL INJURY REPORT")
        print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)

        if team_abbrev:
            injuries = self.get_team_injuries(team_abbrev)
            print(f"\n{team_abbrev} Injuries:")
        else:
            injuries = list(self.injuries.values())
            # Group by team
            by_team = {}
            for injury in injuries:
                team = injury.get('team', 'UNK')
                if team not in by_team:
                    by_team[team] = []
                by_team[team].append(injury)

            for team in sorted(by_team.keys()):
                print(f"\n{team}:")
                for injury in by_team[team]:
                    status = injury.get('status', 'OUT')
                    injury_type = injury.get('injury_type', '')
                    type_str = f" ({injury_type})" if injury_type else ""
                    print(f"  [{status:^10}] {injury['player_name']}{type_str}")
            return

        if not injuries:
            print("  No injuries reported")
            return

        # Group by status
        for status in ['OUT', 'IR', 'LTIR', 'SUSPENDED', 'DTD', 'QUESTIONABLE', 'PROBABLE']:
            status_injuries = [i for i in injuries if i.get('status') == status]
            if status_injuries:
                print(f"\n  {status}:")
                for injury in status_injuries:
                    injury_type = injury.get('injury_type', '')
                    type_str = f" - {injury_type}" if injury_type else ""
                    print(f"    {injury['player_name']}{type_str}")

    def load_from_file(self, filepath: str):
        """
        Load injuries from a text file.

        File format (one per line):
        PlayerName,TEAM,STATUS,InjuryType

        Example:
        Connor McDavid,EDM,OUT,Ankle
        Auston Matthews,TOR,DTD,Upper Body
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                count = 0
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    parts = line.split(',')
                    if len(parts) >= 3:
                        player_name = parts[0].strip()
                        team = parts[1].strip()
                        status = parts[2].strip()
                        injury_type = parts[3].strip() if len(parts) > 3 else ""

                        self.add_injury(player_name, team, status, injury_type)
                        count += 1

            self._save_cache()
            print(f"Loaded {count} injuries from {filepath}")

        except FileNotFoundError:
            print(f"File not found: {filepath}")
        except Exception as e:
            print(f"Error loading injuries: {e}")

    def export_to_file(self, filepath: str):
        """Export current injuries to a file."""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# NHL Injuries Export\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write("# Format: PlayerName,TEAM,STATUS,InjuryType\n\n")

            for injury in sorted(self.injuries.values(),
                                 key=lambda x: (x['team'], x['player_name'])):
                f.write(f"{injury['player_name']},{injury['team']},"
                        f"{injury['status']},{injury.get('injury_type', '')}\n")

        print(f"Exported {len(self.injuries)} injuries to {filepath}")

    def clear_all(self):
        """Clear all injuries."""
        self.injuries = {}
        self._save_cache()
        print("Cleared all injuries")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='NHL Injury Tracker')
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')

    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch injuries from Daily Faceoff')
    fetch_parser.add_argument('--team', '-t', help='Specific team abbreviation (or all)')

    # Add command
    add_parser = subparsers.add_parser('add', help='Manually add an injury')
    add_parser.add_argument('player', help='Player name')
    add_parser.add_argument('team', help='Team abbreviation')
    add_parser.add_argument('--status', '-s', default='OUT',
                           help='Status (OUT, IR, LTIR, DTD, SUSPENDED)')
    add_parser.add_argument('--injury', '-i', default='', help='Injury type')

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a player from injury list')
    remove_parser.add_argument('player', help='Player name')

    # List command
    list_parser = subparsers.add_parser('list', help='List all injuries')
    list_parser.add_argument('--team', '-t', help='Filter by team')

    # Load command
    load_parser = subparsers.add_parser('load', help='Load injuries from file')
    load_parser.add_argument('file', help='Path to injuries file')

    # Export command
    export_parser = subparsers.add_parser('export', help='Export injuries to file')
    export_parser.add_argument('--file', '-f', default='injuries_export.txt',
                              help='Output file path')

    # Clear command
    subparsers.add_parser('clear', help='Clear all injuries')

    # Check command
    check_parser = subparsers.add_parser('check', help='Check if a player is injured')
    check_parser.add_argument('player', help='Player name')

    args = parser.parse_args()
    tracker = InjuryTracker()

    if args.action == 'fetch':
        if args.team:
            tracker.fetch_team_injuries_from_dailyfaceoff(args.team)
        else:
            tracker.fetch_all_injuries()
        tracker.print_injury_report()

    elif args.action == 'add':
        tracker.add_injury_manual(args.player, args.team, args.status, args.injury)

    elif args.action == 'remove':
        tracker.remove_injury(args.player)

    elif args.action == 'list':
        tracker.print_injury_report(args.team)

    elif args.action == 'load':
        tracker.load_from_file(args.file)

    elif args.action == 'export':
        tracker.export_to_file(args.file)

    elif args.action == 'clear':
        tracker.clear_all()

    elif args.action == 'check':
        status = tracker.get_player_status(args.player)
        if status:
            print(f"{status['player_name']} ({status['team']}): {status['status']}")
            if status.get('injury_type'):
                print(f"  Injury: {status['injury_type']}")
        else:
            print(f"{args.player}: No injury reported (available)")

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
