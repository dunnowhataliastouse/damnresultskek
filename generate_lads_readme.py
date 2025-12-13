"""Generate predictions README filtered by Ladbrokes player shots data."""

import sqlite3
import json
from nhl_predictions_enhanced import EnhancedPlayerPredictor
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher

# Try to import injury tracker (optional)
try:
    from injury_tracker import InjuryTracker
    INJURIES_AVAILABLE = True
except ImportError:
    INJURIES_AVAILABLE = False


def load_ladbrokes_data(game_date: str) -> dict:
    """Load all Ladbrokes JSON files for a given date, keyed by matchup."""
    ladbrokes_dir = Path("Ladbrokes")
    pattern = f"ladbrokes_{game_date}_*.json"

    games_data = {}
    for filepath in ladbrokes_dir.glob(pattern):
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            matchup = f"{data['away_team']} @ {data['home_team']}"
            # Create set of player names (normalized) for quick lookup
            player_names = set()
            for p in data.get('players', []):
                player_names.add(normalize_name(p['player']))
            games_data[matchup] = {
                'away_team': data['away_team'],
                'home_team': data['home_team'],
                'players': data.get('players', []),
                'player_names': player_names,
                'filepath': filepath.name
            }
            print(f"Loaded Ladbrokes: {filepath.name} ({len(data.get('players', []))} players)")

    return games_data


def normalize_name(name: str) -> str:
    """Normalize player name for matching."""
    return name.lower().strip().replace('.', '').replace('-', ' ').replace("'", "")


def player_in_ladbrokes(player_name: str, ladbrokes_players: set, threshold: float = 0.85) -> bool:
    """Check if player is in Ladbrokes data using fuzzy matching."""
    normalized = normalize_name(player_name)

    # Exact match
    if normalized in ladbrokes_players:
        return True

    # Fuzzy match
    for lb_name in ladbrokes_players:
        ratio = SequenceMatcher(None, normalized, lb_name).ratio()
        if ratio >= threshold:
            return True

    return False


def get_ladbrokes_odds(player_name: str, ladbrokes_data: dict) -> dict:
    """Get Ladbrokes odds for a player."""
    normalized = normalize_name(player_name)

    for p in ladbrokes_data.get('players', []):
        lb_normalized = normalize_name(p['player'])
        if normalized == lb_normalized:
            return {'line': p.get('line'), 'over': p.get('over_odds'), 'under': p.get('under_odds')}

        # Fuzzy match
        ratio = SequenceMatcher(None, normalized, lb_normalized).ratio()
        if ratio >= 0.85:
            return {'line': p.get('line'), 'over': p.get('over_odds'), 'under': p.get('under_odds')}

    return {}


def generate_lads_readme(game_date: str, output_path: str = "Lads/README.md",
                         include_injuries: bool = True):
    """Generate a formatted README with predictions filtered by Ladbrokes data."""

    # Load Ladbrokes data first
    ladbrokes_games = load_ladbrokes_data(game_date)

    if not ladbrokes_games:
        print(f"No Ladbrokes data found for {game_date}")
        print(f"Looking in: Ladbrokes/ladbrokes_{game_date}_*.json")
        print("Run the userscript extractor first, then save the JSON files.")
        return None

    # Initialize predictor
    predictor = EnhancedPlayerPredictor('nhl_stats.db', use_injuries=include_injuries)
    conn = sqlite3.connect('nhl_stats.db')
    conn.row_factory = sqlite3.Row

    games = conn.execute('''
        SELECT game_id, game_date, home_team_abbrev, away_team_abbrev
        FROM games WHERE game_date = ?
        ORDER BY home_team_abbrev
    ''', (game_date,)).fetchall()

    lines = []
    lines.append("# NHL Shots on Goal Predictions (Ladbrokes Filtered)")
    lines.append(f"## {game_date}")
    lines.append("")
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")
    lines.append("*Only showing players with Ladbrokes SOG markets*")
    lines.append("")

    # Show injury summary if available
    if predictor.injury_tracker and predictor.injury_tracker.injuries:
        lines.append(f"*Injury data loaded: {len(predictor.injury_tracker.injuries)} players on injury report*")
        lines.append("")

    lines.append("---")
    lines.append("")

    all_players = []
    total_lb_players = 0
    matched_players = 0

    for game in games:
        matchup = f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}"
        reverse_matchup = f"{game['home_team_abbrev']} @ {game['away_team_abbrev']}"

        # Check if we have Ladbrokes data for this game (check both directions)
        lb_data = ladbrokes_games.get(matchup) or ladbrokes_games.get(reverse_matchup)

        if not lb_data:
            # Skip games without Ladbrokes data
            continue

        total_lb_players += len(lb_data['players'])

        result = predictor.predict_game_all_players(
            game['home_team_abbrev'],
            game['away_team_abbrev'],
            game['game_date']
        )

        lines.append(f"### {matchup}")
        lines.append(f"*Ladbrokes: {len(lb_data['players'])} players with SOG markets*")
        lines.append("")

        # Combine and filter players - use 1.5 SOG threshold
        players = []
        for p in result['home_players'] + result['away_players']:
            if p['expected_shots'] >= 1.5 and p['player_name'] != 'Unknown':
                # Check if player is in Ladbrokes data
                if player_in_ladbrokes(p['player_name'], lb_data['player_names']):
                    p['matchup'] = matchup
                    # Add Ladbrokes odds
                    odds = get_ladbrokes_odds(p['player_name'], lb_data)
                    p['lb_line'] = odds.get('line')
                    p['lb_over'] = odds.get('over')
                    p['lb_under'] = odds.get('under')
                    players.append(p)
                    matched_players += 1

        # Sort by expected shots descending
        players.sort(key=lambda x: -x['expected_shots'])

        if players:
            lines.append("| Player | Pos | Team | Exp SOG | P(2+) | P(3+) | LB Line | Over | Under | Status |")
            lines.append("|--------|-----|------|---------|-------|-------|---------|------|-------|--------|")

            for p in players:
                prob2 = p['shot_probabilities'].get('2+', 0) * 100
                prob3 = p['shot_probabilities'].get('3+', 0) * 100
                status = p.get('injury_status', '')
                status_str = f"**{status}**" if status else ""
                lb_line = f"{p['lb_line']}" if p.get('lb_line') else "-"
                lb_over = f"{p['lb_over']:.2f}" if p.get('lb_over') else "-"
                lb_under = f"{p['lb_under']:.2f}" if p.get('lb_under') else "-"
                lines.append(f"| {p['player_name']} | {p['position']} | {p['team']} | {p['expected_shots']:.2f} | {prob2:.0f}% | {prob3:.0f}% | {lb_line} | {lb_over} | {lb_under} | {status_str} |")
                all_players.append(p)
        else:
            lines.append("*No matching players found*")

        lines.append("")

    # Summary section
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Games with Ladbrokes data | {len(ladbrokes_games)} |")
    lines.append(f"| Total Ladbrokes players | {total_lb_players} |")
    lines.append(f"| Matched to predictions | {matched_players} |")
    lines.append("")

    # Value picks section - players where our probability differs significantly from odds
    lines.append("---")
    lines.append("")
    lines.append("## Value Analysis")
    lines.append("")

    value_picks = []
    for p in all_players:
        if p.get('lb_line') and p.get('lb_over'):
            line = p['lb_line']
            over_odds = p['lb_over']

            # Get our probability for this line
            if line == 1.5:
                our_prob = p['shot_probabilities'].get('2+', 0)
            elif line == 2.5:
                our_prob = p['shot_probabilities'].get('3+', 0)
            elif line == 0.5:
                our_prob = p['shot_probabilities'].get('1+', 0)
            else:
                continue

            # Calculate implied probability and edge
            implied_prob = 1 / over_odds if over_odds > 0 else 0
            edge = (our_prob - implied_prob) * 100

            if abs(edge) > 5:  # Significant edge
                value_picks.append({
                    'player': p['player_name'],
                    'team': p['team'],
                    'line': line,
                    'our_prob': our_prob * 100,
                    'implied': implied_prob * 100,
                    'edge': edge,
                    'odds': over_odds,
                    'bet': 'OVER' if edge > 0 else 'UNDER'
                })

    if value_picks:
        value_picks.sort(key=lambda x: abs(x['edge']), reverse=True)
        lines.append("| Player | Team | Line | Our % | Implied | Edge | Bet | Odds |")
        lines.append("|--------|------|------|-------|---------|------|-----|------|")
        for v in value_picks[:15]:  # Top 15
            lines.append(f"| {v['player']} | {v['team']} | {v['line']} | {v['our_prob']:.0f}% | {v['implied']:.0f}% | {v['edge']:+.0f}% | {v['bet']} | {v['odds']:.2f} |")
    else:
        lines.append("*No significant value picks found (edge > 5%)*")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Legend")
    lines.append("")
    lines.append("- **Exp SOG**: Expected Shots on Goal (our prediction)")
    lines.append("- **P(2+)**: Our probability of 2+ shots")
    lines.append("- **P(3+)**: Our probability of 3+ shots")
    lines.append("- **LB Line**: Ladbrokes line (e.g., 1.5 = Over/Under 1.5 shots)")
    lines.append("- **Over/Under**: Ladbrokes decimal odds")
    lines.append("- **Edge**: Difference between our probability and implied odds probability")
    lines.append("")

    conn.close()

    # Write to file
    Path(output_path).parent.mkdir(exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"Generated {output_path}")
    print(f"  Games: {len(ladbrokes_games)}")
    print(f"  Players matched: {matched_players}/{total_lb_players}")
    return output_path


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    generate_lads_readme(date)
