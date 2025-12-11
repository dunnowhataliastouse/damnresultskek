"""
NHL Prediction Tracker

Store predictions and compare against actual results.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from nhl_predictions_enhanced import EnhancedPlayerPredictor
from github_committer import GitHubCommitter


class PredictionTracker:
    """Track predictions and compare against actual results."""

    def __init__(self, db_path: str = "nhl_stats.db", predictions_dir: str = "predictions",
                 auto_commit: bool = True):
        self.db_path = db_path
        self.predictions_dir = Path(predictions_dir)
        self.predictions_dir.mkdir(exist_ok=True)
        self.predictor = EnhancedPlayerPredictor(db_path)
        self.auto_commit = auto_commit
        self.committer = GitHubCommitter() if auto_commit else None

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def generate_and_store_predictions(self, game_date: str, min_expected_shots: float = 1.5) -> str:
        """
        Generate predictions for a game date and store them to a JSON file.
        Returns the filename.
        """
        conn = self.get_connection()

        games = conn.execute('''
            SELECT game_id, game_date, home_team_abbrev, away_team_abbrev
            FROM games
            WHERE game_date = ?
            ORDER BY home_team_abbrev
        ''', (game_date,)).fetchall()

        predictions_data = {
            "generated_at": datetime.now().isoformat(),
            "game_date": game_date,
            "games": [],
            "all_predictions": []
        }

        for game in games:
            result = self.predictor.predict_game_all_players(
                game['home_team_abbrev'],
                game['away_team_abbrev'],
                game['game_date']
            )

            game_data = {
                "game_id": game['game_id'],
                "home_team": game['home_team_abbrev'],
                "away_team": game['away_team_abbrev'],
                "matchup": f"{game['away_team_abbrev']} @ {game['home_team_abbrev']}",
                "players": []
            }

            for p in result['home_players'] + result['away_players']:
                if p['expected_shots'] >= min_expected_shots and p['player_name'] != 'Unknown':
                    player_pred = {
                        "player_id": p['player_id'],
                        "player_name": p['player_name'],
                        "position": p['position'],
                        "team": p['team'],
                        "opponent": p['opponent'],
                        "is_home": p['is_home'],
                        "expected_shots": p['expected_shots'],
                        "shots_std": p['shots_std'],
                        "prob_2plus": p['shot_probabilities'].get('2+', 0),
                        "prob_3plus": p['shot_probabilities'].get('3+', 0),
                        "prob_4plus": p['shot_probabilities'].get('4+', 0),
                        "confidence": p['shots_confidence'],
                        "avg_toi_minutes": p.get('avg_toi_minutes', 0),
                        "games_analyzed": p['games_analyzed'],
                        # Variance indicators
                        "historical_avg_shots": p.get('historical_avg_shots', p['expected_shots']),
                        "deviation_from_avg": p.get('deviation_from_avg', 0),
                        "deviation_pct": p.get('deviation_pct', 0),
                        "variance_indicator": p.get('variance_indicator', 'N/A'),
                        # Results will be filled in later
                        "actual_shots": None,
                        "hit_2plus": None,
                        "hit_3plus": None
                    }
                    game_data["players"].append(player_pred)
                    predictions_data["all_predictions"].append({
                        **player_pred,
                        "game_id": game['game_id'],
                        "matchup": game_data["matchup"]
                    })

            predictions_data["games"].append(game_data)

        conn.close()

        # Save to file
        filename = self.predictions_dir / f"predictions_{game_date}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(predictions_data, f, indent=2, ensure_ascii=False)

        print(f"Stored {len(predictions_data['all_predictions'])} predictions to {filename}")
        return str(filename)

    def update_with_actual_results(self, game_date: str) -> Dict[str, Any]:
        """
        Update stored predictions with actual results from completed games.
        """
        filename = self.predictions_dir / f"predictions_{game_date}.json"

        if not filename.exists():
            print(f"No predictions file found for {game_date}")
            return {}

        with open(filename, 'r', encoding='utf-8') as f:
            predictions_data = json.load(f)

        conn = self.get_connection()

        updated_count = 0
        for pred in predictions_data['all_predictions']:
            # Get actual shots from player_game_stats
            actual = conn.execute('''
                SELECT pgs.shots, pgs.toi_seconds
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = ?
                  AND g.game_date = ?
            ''', (pred['player_id'], game_date)).fetchone()

            if actual and actual['shots'] is not None:
                pred['actual_shots'] = actual['shots']
                pred['actual_toi_minutes'] = round(actual['toi_seconds'] / 60, 1) if actual['toi_seconds'] else 0
                pred['hit_2plus'] = actual['shots'] >= 2
                pred['hit_3plus'] = actual['shots'] >= 3
                pred['prediction_error'] = round(pred['expected_shots'] - actual['shots'], 2)
                updated_count += 1

        # Also update the nested game structure
        for game in predictions_data['games']:
            for player in game['players']:
                # Find matching prediction in all_predictions
                matching = next((p for p in predictions_data['all_predictions']
                                if p['player_id'] == player['player_id']), None)
                if matching and matching.get('actual_shots') is not None:
                    player['actual_shots'] = matching['actual_shots']
                    player['actual_toi_minutes'] = matching.get('actual_toi_minutes', 0)
                    player['hit_2plus'] = matching['hit_2plus']
                    player['hit_3plus'] = matching['hit_3plus']
                    player['prediction_error'] = matching['prediction_error']

        predictions_data['results_updated_at'] = datetime.now().isoformat()

        conn.close()

        # Save updated file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(predictions_data, f, indent=2, ensure_ascii=False)

        print(f"Updated {updated_count} predictions with actual results")

        # Auto-commit to GitHub if enabled
        if self.auto_commit and self.committer and updated_count > 0:
            print("\nAuto-committing results to GitHub...")
            self.committer.commit_results(game_date, predictions_data, str(filename))

        return predictions_data

    def compare_predictions(self, game_date: str) -> Dict[str, Any]:
        """
        Compare predictions against actual results and generate accuracy report.
        """
        filename = self.predictions_dir / f"predictions_{game_date}.json"

        if not filename.exists():
            print(f"No predictions file found for {game_date}")
            return {}

        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Filter to only predictions with actual results
        with_results = [p for p in data['all_predictions'] if p.get('actual_shots') is not None]

        if not with_results:
            print(f"No actual results found yet for {game_date}")
            print("Run 'update_with_actual_results()' after games are completed.")
            return {}

        # Calculate metrics
        total = len(with_results)
        errors = [p['prediction_error'] for p in with_results]
        mae = sum(abs(e) for e in errors) / total
        rmse = (sum(e**2 for e in errors) / total) ** 0.5

        # 2+ shots accuracy
        pred_2plus = [p for p in with_results if p['prob_2plus'] > 0.5]
        hit_2plus_correct = sum(1 for p in pred_2plus if p['hit_2plus'])
        hit_2plus_total = len(pred_2plus)

        # 3+ shots accuracy
        pred_3plus = [p for p in with_results if p['prob_3plus'] > 0.5]
        hit_3plus_correct = sum(1 for p in pred_3plus if p['hit_3plus'])
        hit_3plus_total = len(pred_3plus)

        # High confidence picks (>70% for 2+)
        high_conf_2plus = [p for p in with_results if p['prob_2plus'] > 0.7]
        high_conf_2plus_correct = sum(1 for p in high_conf_2plus if p['hit_2plus'])

        report = {
            "game_date": game_date,
            "total_predictions": total,
            "mae": round(mae, 2),
            "rmse": round(rmse, 2),
            "threshold_accuracy": {
                "2plus": {
                    "predicted": hit_2plus_total,
                    "correct": hit_2plus_correct,
                    "accuracy": round(hit_2plus_correct / hit_2plus_total * 100, 1) if hit_2plus_total > 0 else 0
                },
                "3plus": {
                    "predicted": hit_3plus_total,
                    "correct": hit_3plus_correct,
                    "accuracy": round(hit_3plus_correct / hit_3plus_total * 100, 1) if hit_3plus_total > 0 else 0
                },
                "high_confidence_2plus": {
                    "predicted": len(high_conf_2plus),
                    "correct": high_conf_2plus_correct,
                    "accuracy": round(high_conf_2plus_correct / len(high_conf_2plus) * 100, 1) if high_conf_2plus else 0
                }
            },
            "predictions": with_results
        }

        return report

    def print_comparison_report(self, game_date: str):
        """Print a formatted comparison report."""
        report = self.compare_predictions(game_date)

        if not report:
            return

        print(f"\n{'='*70}")
        print(f"PREDICTION VS RESULTS - {game_date}")
        print(f"{'='*70}\n")

        print(f"Total Predictions: {report['total_predictions']}")
        print(f"Mean Absolute Error: {report['mae']} shots")
        print(f"RMSE: {report['rmse']} shots")

        print(f"\n--- Threshold Accuracy ---")
        t2 = report['threshold_accuracy']['2plus']
        print(f"2+ Shots: {t2['correct']}/{t2['predicted']} = {t2['accuracy']}%")

        t3 = report['threshold_accuracy']['3plus']
        print(f"3+ Shots: {t3['correct']}/{t3['predicted']} = {t3['accuracy']}%")

        hc = report['threshold_accuracy']['high_confidence_2plus']
        print(f"High Conf 2+ (>70%): {hc['correct']}/{hc['predicted']} = {hc['accuracy']}%")

        # Print detailed results by matchup
        print(f"\n--- Detailed Results ---\n")

        # Group by matchup
        by_matchup = {}
        for p in report['predictions']:
            matchup = p.get('matchup', 'Unknown')
            if matchup not in by_matchup:
                by_matchup[matchup] = []
            by_matchup[matchup].append(p)

        for matchup, players in by_matchup.items():
            print(f"\n### {matchup}")
            print(f"| Player | Pos | ExpSOG | Actual | Error | P(2+) | Hit? |")
            print(f"|--------|-----|--------|--------|-------|-------|------|")

            # Sort by expected shots descending
            players.sort(key=lambda x: x['expected_shots'], reverse=True)

            for p in players:
                name = p['player_name'][:20]
                pos = p['position']
                exp = p['expected_shots']
                actual = p['actual_shots']
                error = p['prediction_error']
                prob2 = p['prob_2plus'] * 100
                hit = "✓" if p['hit_2plus'] else "✗"
                error_str = f"+{error}" if error > 0 else str(error)
                print(f"| {name} | {pos} | {exp:.2f} | {actual} | {error_str} | {prob2:.0f}% | {hit} |")

        # Best and worst predictions
        sorted_by_error = sorted(report['predictions'], key=lambda x: abs(x['prediction_error']))

        print(f"\n--- Best Predictions (lowest error) ---")
        for p in sorted_by_error[:5]:
            print(f"  {p['player_name']}: Expected {p['expected_shots']:.2f}, Actual {p['actual_shots']} (error: {p['prediction_error']:+.2f})")

        print(f"\n--- Worst Predictions (highest error) ---")
        for p in sorted_by_error[-5:]:
            print(f"  {p['player_name']}: Expected {p['expected_shots']:.2f}, Actual {p['actual_shots']} (error: {p['prediction_error']:+.2f})")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='NHL Prediction Tracker')
    parser.add_argument('action', choices=['store', 'update', 'compare'],
                        help='Action to perform')
    parser.add_argument('--date', '-d', required=True,
                        help='Game date (YYYY-MM-DD)')
    parser.add_argument('--min-shots', type=float, default=1.5,
                        help='Minimum expected shots threshold (default: 1.5)')
    parser.add_argument('--no-commit', action='store_true',
                        help='Disable auto-commit to GitHub (update action only)')

    args = parser.parse_args()

    # Auto-commit is enabled by default, disabled with --no-commit
    auto_commit = not args.no_commit
    tracker = PredictionTracker(auto_commit=auto_commit)

    if args.action == 'store':
        tracker.generate_and_store_predictions(args.date, args.min_shots)

    elif args.action == 'update':
        tracker.update_with_actual_results(args.date)

    elif args.action == 'compare':
        tracker.print_comparison_report(args.date)


if __name__ == '__main__':
    main()
