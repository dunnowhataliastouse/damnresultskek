"""Generate well-formatted predictions README for GitHub."""

import sqlite3
import json
from nhl_predictions_enhanced import EnhancedPlayerPredictor
from datetime import datetime
from pathlib import Path


def load_results_if_available(game_date: str) -> dict:
    """Load prediction results if they exist."""
    results_file = Path(f"predictions/predictions_{game_date}.json")
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Check if results have been added
            all_preds = data.get("all_predictions", [])
            if all_preds and any(p.get("actual_shots") is not None for p in all_preds):
                return data
    return None


def generate_predictions_readme(game_date: str, output_path: str = "predictions/README.md"):
    """Generate a formatted README with predictions."""

    predictor = EnhancedPlayerPredictor('nhl_stats.db')
    conn = sqlite3.connect('nhl_stats.db')
    conn.row_factory = sqlite3.Row

    games = conn.execute('''
        SELECT game_id, game_date, home_team_abbrev, away_team_abbrev
        FROM games WHERE game_date = ?
        ORDER BY home_team_abbrev
    ''', (game_date,)).fetchall()

    lines = []
    lines.append("# NHL Shots on Goal Predictions")
    lines.append(f"## {game_date}")
    lines.append("")
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    lines.append("")
    lines.append("---")
    lines.append("")

    all_players = []

    for game in games:
        result = predictor.predict_game_all_players(
            game['home_team_abbrev'],
            game['away_team_abbrev'],
            game['game_date']
        )

        matchup = f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}"
        lines.append(f"### {matchup}")
        lines.append("")

        # Combine and filter players
        players = []
        for p in result['home_players'] + result['away_players']:
            if p['expected_shots'] >= 1.5 and p['player_name'] != 'Unknown':
                p['matchup'] = matchup
                players.append(p)

        # Sort by position then expected shots
        pos_order = {'C': 0, 'L': 1, 'R': 2, 'D': 3}
        players.sort(key=lambda x: (pos_order.get(x['position'], 4), -x['expected_shots']))

        if players:
            lines.append("| Player | Pos | Team | Exp SOG | Hist Avg | Variance | P(2+) | P(3+) |")
            lines.append("|--------|-----|------|---------|----------|----------|-------|-------|")

            for p in players:
                prob2 = p['shot_probabilities'].get('2+', 0) * 100
                prob3 = p['shot_probabilities'].get('3+', 0) * 100
                hist_avg = p.get('historical_avg_shots', p['expected_shots'])
                variance = p.get('variance_indicator', 'N/A')
                lines.append(f"| {p['player_name']} | {p['position']} | {p['team']} | {p['expected_shots']:.2f} | {hist_avg:.2f} | {variance} | {prob2:.0f}% | {prob3:.0f}% |")
                all_players.append(p)
        else:
            lines.append("*No players meeting criteria*")

        lines.append("")

    # Top picks section
    lines.append("---")
    lines.append("")
    lines.append("## Top 15 Shooters (Highest Expected SOG)")
    lines.append("")
    all_players.sort(key=lambda x: -x['expected_shots'])
    lines.append("| Rank | Player | Team | Matchup | Exp SOG | Hist Avg | Variance | P(2+) |")
    lines.append("|------|--------|------|---------|---------|----------|----------|-------|")
    for i, p in enumerate(all_players[:15], 1):
        prob2 = p['shot_probabilities'].get('2+', 0) * 100
        hist_avg = p.get('historical_avg_shots', p['expected_shots'])
        variance = p.get('variance_indicator', 'N/A')
        opp = p['opponent']
        loc = 'vs' if p['is_home'] else '@'
        lines.append(f"| {i} | {p['player_name']} | {p['team']} | {loc} {opp} | {p['expected_shots']:.2f} | {hist_avg:.2f} | {variance} | {prob2:.0f}% |")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## High Confidence Picks (P(2+) > 70%)")
    lines.append("")
    high_conf = [p for p in all_players if p['shot_probabilities'].get('2+', 0) >= 0.70]
    high_conf.sort(key=lambda x: -x['shot_probabilities'].get('2+', 0))
    if high_conf:
        lines.append("| Player | Team | Exp SOG | Hist Avg | Variance | P(2+) |")
        lines.append("|--------|------|---------|----------|----------|-------|")
        for p in high_conf:
            prob2 = p['shot_probabilities'].get('2+', 0) * 100
            hist_avg = p.get('historical_avg_shots', p['expected_shots'])
            variance = p.get('variance_indicator', 'N/A')
            lines.append(f"| {p['player_name']} | {p['team']} | {p['expected_shots']:.2f} | {hist_avg:.2f} | {variance} | {prob2:.0f}% |")
    else:
        lines.append("*No players with >70% probability for 2+ shots.*")

    # Check if results are available and add results section
    results_data = load_results_if_available(game_date)
    if results_data:
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("## Results vs Predictions")
        lines.append("")

        evaluated = [p for p in results_data.get("all_predictions", [])
                    if p.get("actual_shots") is not None]

        if evaluated:
            # Calculate metrics
            errors = [abs(p.get("prediction_error", 0)) for p in evaluated
                     if p.get("prediction_error") is not None]
            mae = sum(errors) / len(errors) if errors else 0
            rmse = (sum(e**2 for e in errors) / len(errors)) ** 0.5 if errors else 0

            # 2+ shots accuracy
            pred_2plus = [p for p in evaluated if p.get("prob_2plus", 0) >= 0.5]
            hit_2plus_correct = sum(1 for p in pred_2plus if p.get("hit_2plus"))
            accuracy_2plus = (hit_2plus_correct / len(pred_2plus) * 100) if pred_2plus else 0

            # 3+ shots accuracy
            pred_3plus = [p for p in evaluated if p.get("prob_3plus", 0) >= 0.5]
            hit_3plus_correct = sum(1 for p in pred_3plus if p.get("hit_3plus"))
            accuracy_3plus = (hit_3plus_correct / len(pred_3plus) * 100) if pred_3plus else 0

            lines.append("### Summary Statistics")
            lines.append("")
            lines.append(f"| Metric | Value |")
            lines.append(f"|--------|-------|")
            lines.append(f"| Total Predictions | {len(evaluated)} |")
            lines.append(f"| Mean Absolute Error | {mae:.2f} shots |")
            lines.append(f"| RMSE | {rmse:.2f} shots |")
            lines.append(f"| 2+ Shots Accuracy | {hit_2plus_correct}/{len(pred_2plus)} ({accuracy_2plus:.0f}%) |")
            lines.append(f"| 3+ Shots Accuracy | {hit_3plus_correct}/{len(pred_3plus)} ({accuracy_3plus:.0f}%) |")
            lines.append("")

            # Best predictions (lowest error)
            sorted_by_error = sorted(evaluated, key=lambda x: abs(x.get("prediction_error", 0)))
            lines.append("### Best Predictions (Lowest Error)")
            lines.append("")
            lines.append("| Player | Team | Expected | Actual | Error | Variance |")
            lines.append("|--------|------|----------|--------|-------|----------|")
            for p in sorted_by_error[:5]:
                error = p.get("prediction_error", 0)
                error_str = f"+{error:.2f}" if error > 0 else f"{error:.2f}"
                variance = p.get("variance_indicator", "N/A")
                lines.append(f"| {p['player_name']} | {p['team']} | {p['expected_shots']:.2f} | {p['actual_shots']} | {error_str} | {variance} |")
            lines.append("")

            # Worst predictions (highest error)
            lines.append("### Worst Predictions (Highest Error)")
            lines.append("")
            lines.append("| Player | Team | Expected | Actual | Error | Variance |")
            lines.append("|--------|------|----------|--------|-------|----------|")
            for p in sorted_by_error[-5:]:
                error = p.get("prediction_error", 0)
                error_str = f"+{error:.2f}" if error > 0 else f"{error:.2f}"
                variance = p.get("variance_indicator", "N/A")
                lines.append(f"| {p['player_name']} | {p['team']} | {p['expected_shots']:.2f} | {p['actual_shots']} | {error_str} | {variance} |")
            lines.append("")

            # Full results table by matchup
            lines.append("### Full Results by Matchup")
            lines.append("")

            # Group by matchup
            by_matchup = {}
            for p in evaluated:
                matchup = p.get('matchup', 'Unknown')
                if matchup not in by_matchup:
                    by_matchup[matchup] = []
                by_matchup[matchup].append(p)

            for matchup, players in by_matchup.items():
                lines.append(f"#### {matchup}")
                lines.append("")
                lines.append("| Player | Pos | Exp | Actual | Error | P(2+) | Hit? | Variance |")
                lines.append("|--------|-----|-----|--------|-------|-------|------|----------|")

                players.sort(key=lambda x: x['expected_shots'], reverse=True)
                for p in players:
                    error = p.get("prediction_error", 0)
                    error_str = f"+{error:.2f}" if error > 0 else f"{error:.2f}"
                    prob2 = p.get("prob_2plus", 0) * 100
                    hit = "✓" if p.get("hit_2plus") else "✗"
                    variance = p.get("variance_indicator", "N/A")
                    lines.append(f"| {p['player_name']} | {p['position']} | {p['expected_shots']:.2f} | {p['actual_shots']} | {error_str} | {prob2:.0f}% | {hit} | {variance} |")
                lines.append("")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Legend")
    lines.append("")
    lines.append("### Columns")
    lines.append("- **Exp SOG**: Expected Shots on Goal (prediction)")
    lines.append("- **Hist Avg**: Historical average SOG over recent games")
    lines.append("- **Variance**: How prediction compares to historical average")
    lines.append("- **P(2+)**: Probability of 2 or more shots")
    lines.append("- **P(3+)**: Probability of 3 or more shots")
    lines.append("")
    lines.append("### Variance Indicators")
    lines.append("| Indicator | Meaning |")
    lines.append("|-----------|---------|")
    lines.append("| **Stable** | Prediction close to historical average (consistent player) |")
    lines.append("| **Slight+** | Prediction 10-20% above historical average |")
    lines.append("| **Slight-** | Prediction 10-20% below historical average |")
    lines.append("| **Higher** | Prediction >20% above historical average |")
    lines.append("| **Lower** | Prediction >20% below historical average |")
    lines.append("| **Variable** | Player has moderate game-to-game variance |")
    lines.append("| **Higher*** | Above average but inconsistent (use caution) |")
    lines.append("| **Lower*** | Below average but inconsistent (use caution) |")
    lines.append("| **Volatile** | High variance player - unpredictable |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Predictions generated using EnhancedPlayerPredictor with position-specific TOI filtering*")
    lines.append("- Forwards (C, L, R): 14+ min average TOI")
    lines.append("- Defensemen (D): 16+ min average TOI")

    conn.close()

    # Write to file
    Path(output_path).parent.mkdir(exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"Generated {output_path}")
    return output_path


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2025-12-11"
    generate_predictions_readme(date)
