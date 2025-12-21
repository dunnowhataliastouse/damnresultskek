"""
NHL Enhanced Prediction Model
Incorporates rest days, goalie performance, and player line chemistry.
"""

import sqlite3
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import statistics


class LineChemistryAnalyzer:
    """
    Analyzes player combinations that produce goals together.
    Detects effective offensive lines and defensive pairings.
    """

    def __init__(self, db_path: str = "nhl_stats.db", injury_tracker=None):
        self.db_path = db_path
        self.injury_tracker = injury_tracker
        # Cache for player combinations: {team: {(p1, p2): {"goals": X, "games": Y}}}
        self.offensive_combos: Dict[str, Dict[Tuple[int, int], Dict]] = defaultdict(lambda: defaultdict(lambda: {"goals": 0, "games": 0, "assists": 0}))
        self.defensive_effectiveness: Dict[str, Dict[Tuple[int, int], Dict]] = defaultdict(lambda: defaultdict(lambda: {"goals_against": 0, "games": 0}))
        # Defensive pairing stats
        self.defensive_pairings: Dict[str, Dict] = {}
        self.team_defense_ratings: Dict[str, float] = {}
        # Player name cache for injury lookups
        self._player_name_cache: Dict[int, str] = {}

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def set_injury_tracker(self, injury_tracker):
        """Set or update the injury tracker."""
        self.injury_tracker = injury_tracker

    def _get_player_name(self, player_id: int) -> str:
        """Get player name from ID, with caching."""
        if player_id in self._player_name_cache:
            return self._player_name_cache[player_id]

        with self.get_connection() as conn:
            result = conn.execute(
                "SELECT full_name FROM players WHERE player_id = ?",
                (player_id,)
            ).fetchone()
            name = result["full_name"] if result else ""
            self._player_name_cache[player_id] = name
            return name

    def _is_player_out(self, player_id: int) -> bool:
        """Check if a player is OUT (unavailable) due to injury."""
        if not self.injury_tracker:
            return False

        player_name = self._get_player_name(player_id)
        if not player_name:
            return False

        is_available, weight = self.injury_tracker.is_player_available(player_name)
        # Player is OUT if not available (weight = 0.0 means OUT/IR/LTIR/SUSPENDED)
        return not is_available or weight == 0.0

    def analyze_goal_combinations(self, team_abbrev: str, before_date: str, lookback_days: int = 60) -> Dict[str, Any]:
        """
        Analyze which player combinations produce goals together.
        Uses play-by-play data to find scorer + assist combos.
        """
        start_date = (datetime.strptime(before_date, "%Y-%m-%d") -
                     timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            # Get all goals with assists for this team
            goals = conn.execute("""
                SELECT
                    pbp.game_id,
                    pbp.player_1_id as scorer,
                    pbp.player_2_id as assist1,
                    pbp.player_3_id as assist2,
                    g.game_date
                FROM play_by_play pbp
                JOIN games g ON pbp.game_id = g.game_id
                WHERE pbp.event_type = 'goal'
                  AND pbp.team_abbrev = ?
                  AND g.game_date >= ? AND g.game_date < ?
                ORDER BY g.game_date
            """, (team_abbrev, start_date, before_date)).fetchall()

        if not goals:
            return {"top_combos": [], "total_goals": 0}

        # Track combinations
        combo_goals = defaultdict(int)
        combo_games = defaultdict(set)

        for goal in goals:
            scorer = goal["scorer"]
            assist1 = goal["assist1"]
            assist2 = goal["assist2"]
            game_id = goal["game_id"]

            # Scorer-Assist1 combo
            if scorer and assist1:
                key = tuple(sorted([scorer, assist1]))
                combo_goals[key] += 1
                combo_games[key].add(game_id)

            # Scorer-Assist2 combo
            if scorer and assist2:
                key = tuple(sorted([scorer, assist2]))
                combo_goals[key] += 1
                combo_games[key].add(game_id)

            # Assist1-Assist2 combo (often same line)
            if assist1 and assist2:
                key = tuple(sorted([assist1, assist2]))
                combo_goals[key] += 1
                combo_games[key].add(game_id)

        # Calculate combo effectiveness
        combos = []
        for combo, goals_count in combo_goals.items():
            games_together = len(combo_games[combo])
            if games_together >= 3:  # Minimum sample
                combos.append({
                    "players": combo,
                    "goals": goals_count,
                    "games": games_together,
                    "goals_per_game": goals_count / games_together
                })

        # Sort by goals per game
        combos.sort(key=lambda x: x["goals_per_game"], reverse=True)

        # Store in cache
        for combo in combos:
            self.offensive_combos[team_abbrev][combo["players"]] = {
                "goals": combo["goals"],
                "games": combo["games"],
                "goals_per_game": combo["goals_per_game"]
            }

        return {
            "top_combos": combos[:10],
            "total_goals": len(goals),
            "unique_combos": len(combos)
        }

    def get_team_line_strength(self, team_abbrev: str, before_date: str) -> float:
        """
        Calculate overall line chemistry strength for a team.
        Higher = more consistent productive combinations.
        Excludes combos involving injured (OUT) players.
        """
        if team_abbrev not in self.offensive_combos:
            self.analyze_goal_combinations(team_abbrev, before_date)

        combos = self.offensive_combos[team_abbrev]
        if not combos:
            return 0.5  # Neutral

        # Filter out combos involving injured players
        available_combos = {}
        for combo_key, combo_data in combos.items():
            player1_id, player2_id = combo_key
            # Skip if either player is injured/OUT
            if self._is_player_out(player1_id) or self._is_player_out(player2_id):
                continue
            available_combos[combo_key] = combo_data

        if not available_combos:
            return 0.4  # Slightly below neutral if all top combos injured

        # Calculate weighted average of top combos' effectiveness
        top_combos = sorted(available_combos.items(), key=lambda x: x[1]["goals"], reverse=True)[:5]

        if not top_combos:
            return 0.5

        total_goals = sum(c[1]["goals"] for c in top_combos)
        total_games = sum(c[1]["games"] for c in top_combos)

        if total_games == 0:
            return 0.5

        # Goals per game for top combos (league avg ~0.3-0.5 per combo)
        gpg = total_goals / total_games

        # Normalize to 0-1 scale (0.2 = weak, 0.8 = strong)
        return min(1.0, max(0.0, (gpg - 0.1) / 0.6))

    def analyze_defensive_pairings(self, team_abbrev: str, before_date: str, lookback_days: int = 60) -> Dict[str, Any]:
        """
        Analyze defensive pairing effectiveness.
        Uses plus/minus, blocked shots, and goals against when on ice.
        """
        start_date = (datetime.strptime(before_date, "%Y-%m-%d") -
                     timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            # Get defensemen game stats
            defensemen = conn.execute("""
                SELECT
                    pgs.player_id,
                    pgs.game_id,
                    pgs.plus_minus,
                    pgs.blocked_shots,
                    pgs.toi_seconds,
                    p.full_name
                FROM player_game_stats pgs
                JOIN players p ON pgs.player_id = p.player_id
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.team_abbrev = ?
                  AND p.position = 'D'
                  AND g.game_date >= ? AND g.game_date < ?
                  AND pgs.toi_seconds > 600
                ORDER BY g.game_date, pgs.toi_seconds DESC
            """, (team_abbrev, start_date, before_date)).fetchall()

        if not defensemen:
            return {"defense_rating": 0.5, "top_defensemen": []}

        # Group by game to find likely pairings (top 2, 4, 6 TOI)
        games = defaultdict(list)
        for d in defensemen:
            games[d["game_id"]].append({
                "player_id": d["player_id"],
                "name": d["full_name"],
                "plus_minus": d["plus_minus"] or 0,
                "blocked_shots": d["blocked_shots"] or 0,
                "toi": d["toi_seconds"] or 0
            })

        # Calculate individual defenseman ratings
        player_stats = defaultdict(lambda: {"plus_minus": 0, "blocks": 0, "games": 0, "toi": 0})

        for game_id, players in games.items():
            # Sort by TOI to get likely top pairings
            players.sort(key=lambda x: x["toi"], reverse=True)
            for p in players[:6]:  # Top 6 defensemen
                player_stats[p["player_id"]]["plus_minus"] += p["plus_minus"]
                player_stats[p["player_id"]]["blocks"] += p["blocked_shots"]
                player_stats[p["player_id"]]["games"] += 1
                player_stats[p["player_id"]]["toi"] += p["toi"]

        # Calculate team defense rating
        total_plus_minus = sum(s["plus_minus"] for s in player_stats.values())
        total_blocks = sum(s["blocks"] for s in player_stats.values())
        total_games = sum(s["games"] for s in player_stats.values()) / 6 if player_stats else 1

        # Normalize: +/- per game and blocks per game
        pm_per_game = total_plus_minus / max(1, total_games)
        blocks_per_game = total_blocks / max(1, total_games)

        # Defense rating: combine +/- and shot blocking
        # League avg: ~0 +/-, ~15 blocks/game for D corps
        defense_rating = 0.5 + (pm_per_game * 0.02) + ((blocks_per_game - 15) * 0.01)
        defense_rating = max(0.2, min(0.8, defense_rating))

        self.team_defense_ratings[team_abbrev] = defense_rating
        self.defensive_pairings[team_abbrev] = player_stats

        return {
            "defense_rating": defense_rating,
            "plus_minus_per_game": pm_per_game,
            "blocks_per_game": blocks_per_game,
            "defensemen_tracked": len(player_stats)
        }

    def get_defense_rating(self, team_abbrev: str, before_date: str) -> float:
        """
        Get team's defensive rating, excluding injured defensemen.
        """
        if team_abbrev not in self.defensive_pairings:
            self.analyze_defensive_pairings(team_abbrev, before_date)

        player_stats = self.defensive_pairings.get(team_abbrev, {})
        if not player_stats:
            return 0.5

        # Filter out injured defensemen from the calculation
        available_stats = {}
        for player_id, stats in player_stats.items():
            if not self._is_player_out(player_id):
                available_stats[player_id] = stats

        if not available_stats:
            return 0.4  # Below neutral if all defensemen are injured

        # Recalculate defense rating with only available players
        total_plus_minus = sum(s["plus_minus"] for s in available_stats.values())
        total_blocks = sum(s["blocks"] for s in available_stats.values())
        total_games = sum(s["games"] for s in available_stats.values()) / max(1, len(available_stats))

        # Normalize: +/- per game and blocks per game
        pm_per_game = total_plus_minus / max(1, total_games)
        blocks_per_game = total_blocks / max(1, total_games)

        # Defense rating: combine +/- and shot blocking
        # League avg: ~0 +/-, ~15 blocks/game for D corps
        defense_rating = 0.5 + (pm_per_game * 0.02) + ((blocks_per_game - 15) * 0.01)
        defense_rating = max(0.2, min(0.8, defense_rating))

        return defense_rating

    def analyze_offense_vs_specific_defense(self, attacking_team: str, defending_team: str,
                                             before_date: str, lookback_days: int = 120) -> Dict[str, Any]:
        """
        Analyze how attacking team's offense performs specifically against defending team.
        Returns historical matchup performance.
        """
        start_date = (datetime.strptime(before_date, "%Y-%m-%d") -
                     timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            # Get goals scored by attacking team against defending team
            matchup_goals = conn.execute("""
                SELECT
                    pbp.game_id,
                    pbp.player_1_id as scorer,
                    pbp.player_2_id as assist1,
                    pbp.player_3_id as assist2,
                    g.game_date,
                    g.home_team_abbrev,
                    g.away_team_abbrev,
                    g.home_score,
                    g.away_score
                FROM play_by_play pbp
                JOIN games g ON pbp.game_id = g.game_id
                WHERE pbp.event_type = 'goal'
                  AND pbp.team_abbrev = ?
                  AND ((g.home_team_abbrev = ? AND g.away_team_abbrev = ?)
                    OR (g.home_team_abbrev = ? AND g.away_team_abbrev = ?))
                  AND g.game_date >= ? AND g.game_date < ?
                ORDER BY g.game_date
            """, (attacking_team, attacking_team, defending_team, defending_team, attacking_team,
                  start_date, before_date)).fetchall()

            # Get games between these teams
            matchup_games = conn.execute("""
                SELECT
                    game_id,
                    game_date,
                    home_team_abbrev,
                    away_team_abbrev,
                    home_score,
                    away_score
                FROM games
                WHERE ((home_team_abbrev = ? AND away_team_abbrev = ?)
                    OR (home_team_abbrev = ? AND away_team_abbrev = ?))
                  AND game_date >= ? AND game_date < ?
                  AND home_score IS NOT NULL
            """, (attacking_team, defending_team, defending_team, attacking_team,
                  start_date, before_date)).fetchall()

        if not matchup_games:
            return {
                "games_played": 0,
                "goals_per_game": 0,
                "win_rate": 0.5,
                "offensive_rating_vs_opponent": 0.5
            }

        # Calculate attacking team's performance against this specific opponent
        total_goals = 0
        wins = 0
        for game in matchup_games:
            if game["home_team_abbrev"] == attacking_team:
                total_goals += game["home_score"]
                if game["home_score"] > game["away_score"]:
                    wins += 1
            else:
                total_goals += game["away_score"]
                if game["away_score"] > game["home_score"]:
                    wins += 1

        goals_per_game = total_goals / len(matchup_games)
        win_rate = wins / len(matchup_games)

        # Track which combos score against this opponent
        combo_vs_opponent = defaultdict(int)
        for goal in matchup_goals:
            scorer = goal["scorer"]
            assist1 = goal["assist1"]
            if scorer and assist1:
                key = tuple(sorted([scorer, assist1]))
                combo_vs_opponent[key] += 1

        # Rating: higher if team scores well against this opponent
        # League avg is ~3.0 goals per game
        offensive_rating = 0.5 + (goals_per_game - 3.0) * 0.08
        offensive_rating = max(0.25, min(0.75, offensive_rating))

        return {
            "games_played": len(matchup_games),
            "goals_per_game": round(goals_per_game, 2),
            "win_rate": round(win_rate, 3),
            "offensive_rating_vs_opponent": round(offensive_rating, 3),
            "top_combos_vs_opponent": len(combo_vs_opponent)
        }

    def get_bidirectional_matchup(self, home_team: str, away_team: str,
                                   before_date: str) -> Dict[str, Any]:
        """
        Get full bidirectional matchup analysis:
        - Home offense vs Away defense
        - Away offense vs Home defense
        Returns matchup advantage scores for prediction.
        """
        # Home team's offense vs Away team's defense
        home_offense_vs_away = self.analyze_offense_vs_specific_defense(
            home_team, away_team, before_date)

        # Away team's offense vs Home team's defense
        away_offense_vs_home = self.analyze_offense_vs_specific_defense(
            away_team, home_team, before_date)

        # Get general team strengths
        home_line_strength = self.get_team_line_strength(home_team, before_date)
        away_line_strength = self.get_team_line_strength(away_team, before_date)
        home_defense = self.get_defense_rating(home_team, before_date)
        away_defense = self.get_defense_rating(away_team, before_date)

        # Calculate matchup-specific advantages
        # Home offensive advantage: How well home offense does vs away D, minus home's general offense
        home_matchup_edge = 0.0
        away_matchup_edge = 0.0

        # If we have head-to-head data, use it
        if home_offense_vs_away["games_played"] >= 1:
            # Compare team's offense vs this opponent to their general offense
            home_goals_vs_opp = home_offense_vs_away["goals_per_game"]
            # Weight by sample size
            sample_weight = min(1.0, home_offense_vs_away["games_played"] / 4)
            home_matchup_edge = (home_offense_vs_away["offensive_rating_vs_opponent"] - 0.5) * sample_weight

        if away_offense_vs_home["games_played"] >= 1:
            sample_weight = min(1.0, away_offense_vs_home["games_played"] / 4)
            away_matchup_edge = (away_offense_vs_home["offensive_rating_vs_opponent"] - 0.5) * sample_weight

        # Combined matchup advantage (positive = home advantage)
        # Home wants: high home offense vs away D, low away offense vs home D
        matchup_advantage = home_matchup_edge - away_matchup_edge

        # Also calculate style matchup using general ratings
        # Strong offense vs weak defense = good matchup
        home_style_edge = home_line_strength - away_defense
        away_style_edge = away_line_strength - home_defense
        style_advantage = home_style_edge - away_style_edge

        return {
            "home_offense_vs_away_defense": home_offense_vs_away,
            "away_offense_vs_home_defense": away_offense_vs_home,
            "home_line_strength": round(home_line_strength, 3),
            "away_line_strength": round(away_line_strength, 3),
            "home_defense_rating": round(home_defense, 3),
            "away_defense_rating": round(away_defense, 3),
            "h2h_matchup_advantage": round(matchup_advantage, 3),
            "style_matchup_advantage": round(style_advantage, 3),
            "combined_matchup_advantage": round((matchup_advantage * 0.6 + style_advantage * 0.4), 3),
            "has_h2h_data": home_offense_vs_away["games_played"] > 0 or away_offense_vs_home["games_played"] > 0
        }

    def get_matchup_advantage(self, attacking_team: str, defending_team: str,
                              before_date: str) -> Dict[str, float]:
        """
        Calculate how well attacking team's lines match up vs defending team.
        Now includes defensive pairing analysis and H2H matchup data.
        """
        # Get attacking team's top scoring combos
        if attacking_team not in self.offensive_combos:
            self.analyze_goal_combinations(attacking_team, before_date)

        attack_strength = self.get_team_line_strength(attacking_team, before_date)

        # Get defensive ratings
        attack_defense = self.get_defense_rating(attacking_team, before_date)
        defend_defense = self.get_defense_rating(defending_team, before_date)

        # Get H2H offense vs defense
        h2h_matchup = self.analyze_offense_vs_specific_defense(attacking_team, defending_team, before_date)

        # Matchup advantage considers:
        # 1. Attacking team's offensive chemistry vs opponent defense
        # 2. Attacking team's defensive strength
        # 3. Historical performance vs this specific opponent
        offense_vs_defense = attack_strength - (defend_defense * 0.5)

        # Add H2H bonus if we have data
        h2h_bonus = 0.0
        if h2h_matchup["games_played"] >= 1:
            h2h_bonus = (h2h_matchup["offensive_rating_vs_opponent"] - 0.5) * 0.3

        return {
            "attack_line_strength": attack_strength,
            "attack_defense_rating": attack_defense,
            "opponent_defense_rating": defend_defense,
            "matchup_edge": offense_vs_defense + h2h_bonus,
            "defense_matchup": attack_defense - defend_defense,
            "h2h_offensive_rating": h2h_matchup["offensive_rating_vs_opponent"],
            "h2h_games": h2h_matchup["games_played"]
        }


class GoalieAnalyzer:
    """Analyzes goalie performance and matchup factors."""

    def __init__(self, db_path: str = "nhl_stats.db"):
        self.db_path = db_path

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_team_goalie_stats(self, team_abbrev: str, before_date: str) -> Dict[str, Any]:
        """Get the starting goalie stats for a team."""
        with self.get_connection() as conn:
            # Get goalie with most recent starts
            goalie = conn.execute("""
                SELECT
                    gs.player_id,
                    p.full_name,
                    gs.games_played,
                    gs.wins,
                    gs.losses,
                    gs.save_pct,
                    gs.gaa,
                    gs.shutouts,
                    gs.quality_starts,
                    gs.quality_start_pct
                FROM goalie_season_stats gs
                JOIN players p ON gs.player_id = p.player_id
                WHERE gs.team_abbrev = ?
                ORDER BY gs.games_played DESC
                LIMIT 1
            """, (team_abbrev,)).fetchone()

        if not goalie:
            return {
                "goalie_name": "Unknown",
                "save_pct": 0.900,
                "gaa": 3.0,
                "games_played": 0,
                "quality_start_pct": 0.5,
                "win_pct": 0.5
            }

        games = goalie["games_played"] or 1
        wins = goalie["wins"] or 0

        return {
            "goalie_id": goalie["player_id"],
            "goalie_name": goalie["full_name"],
            "save_pct": goalie["save_pct"] or 0.900,
            "gaa": goalie["gaa"] or 3.0,
            "games_played": games,
            "quality_start_pct": goalie["quality_start_pct"] or 0.5,
            "win_pct": wins / games if games > 0 else 0.5,
            "shutouts": goalie["shutouts"] or 0
        }

    def get_goalie_recent_form(self, team_abbrev: str, before_date: str,
                               games: int = 5) -> Dict[str, Any]:
        """Get goalie's recent form from game stats."""
        with self.get_connection() as conn:
            recent = conn.execute("""
                SELECT
                    ggs.saves,
                    ggs.shots_against,
                    ggs.goals_against,
                    g.game_date
                FROM goalie_game_stats ggs
                JOIN games g ON ggs.game_id = g.game_id
                WHERE ggs.team_abbrev = ?
                  AND g.game_date < ?
                ORDER BY g.game_date DESC
                LIMIT ?
            """, (team_abbrev, before_date, games)).fetchall()

        if not recent:
            return {"recent_save_pct": 0.900, "recent_gaa": 3.0, "recent_games": 0}

        total_saves = sum(r["saves"] or 0 for r in recent)
        total_shots = sum(r["shots_against"] or 0 for r in recent)
        total_goals = sum(r["goals_against"] or 0 for r in recent)

        return {
            "recent_save_pct": total_saves / total_shots if total_shots > 0 else 0.900,
            "recent_gaa": total_goals / len(recent) if recent else 3.0,
            "recent_games": len(recent)
        }

    def get_goalie_matchup_factor(self, home_team: str, away_team: str,
                                  before_date: str) -> Dict[str, float]:
        """Calculate goalie advantage between two teams."""
        home_goalie = self.get_team_goalie_stats(home_team, before_date)
        away_goalie = self.get_team_goalie_stats(away_team, before_date)

        home_recent = self.get_goalie_recent_form(home_team, before_date)
        away_recent = self.get_goalie_recent_form(away_team, before_date)

        # Combine season and recent stats
        home_sv_pct = 0.6 * home_goalie["save_pct"] + 0.4 * home_recent["recent_save_pct"]
        away_sv_pct = 0.6 * away_goalie["save_pct"] + 0.4 * away_recent["recent_save_pct"]

        # Save percentage differential (each 1% = ~0.3 goals per game)
        sv_pct_diff = home_sv_pct - away_sv_pct

        # Quality start likelihood
        home_qs = home_goalie["quality_start_pct"]
        away_qs = away_goalie["quality_start_pct"]

        return {
            "home_goalie_save_pct": home_sv_pct,
            "away_goalie_save_pct": away_sv_pct,
            "goalie_advantage": sv_pct_diff * 30,  # Scale to reasonable range
            "home_quality_start_pct": home_qs,
            "away_quality_start_pct": away_qs
        }


class RestDaysAnalyzer:
    """Analyzes rest days and schedule factors with enhanced back-to-back detection."""

    def __init__(self, db_path: str = "nhl_stats.db"):
        self.db_path = db_path

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_rest_days(self, team_abbrev: str, game_date: str) -> Dict[str, Any]:
        """Calculate rest days since last game for a team."""
        with self.get_connection() as conn:
            # Get last 3 games to detect patterns
            last_games = conn.execute("""
                SELECT game_date
                FROM games
                WHERE (home_team_abbrev = ? OR away_team_abbrev = ?)
                  AND game_date < ?
                  AND home_score IS NOT NULL
                ORDER BY game_date DESC
                LIMIT 3
            """, (team_abbrev, team_abbrev, game_date)).fetchall()

        if not last_games:
            return {
                "rest_days": 3,
                "is_back_to_back": False,
                "is_well_rested": True,
                "is_third_in_four": False,
                "fatigue_factor": 0.0
            }

        game_dt = datetime.strptime(game_date, "%Y-%m-%d")
        last_dt = datetime.strptime(last_games[0]["game_date"], "%Y-%m-%d")
        rest_days = (game_dt - last_dt).days - 1  # Days between games

        # Check for 3rd game in 4 nights
        is_third_in_four = False
        if len(last_games) >= 2:
            second_last_dt = datetime.strptime(last_games[1]["game_date"], "%Y-%m-%d")
            days_span = (game_dt - second_last_dt).days
            if days_span <= 3:  # 3 games in 4 days
                is_third_in_four = True

        # Fatigue factor: higher = more tired
        # Back-to-back = 1.0, 1 day rest = 0.5, 2+ days = 0.0
        if rest_days == 0:
            fatigue_factor = 1.0
        elif rest_days == 1:
            fatigue_factor = 0.4
        else:
            fatigue_factor = 0.0

        # Add extra fatigue for 3rd in 4
        if is_third_in_four:
            fatigue_factor = min(1.0, fatigue_factor + 0.3)

        return {
            "rest_days": rest_days,
            "is_back_to_back": rest_days == 0,
            "is_well_rested": rest_days >= 2,
            "is_third_in_four": is_third_in_four,
            "fatigue_factor": fatigue_factor
        }

    def get_schedule_difficulty(self, team_abbrev: str, game_date: str,
                                lookback: int = 7) -> Dict[str, Any]:
        """Calculate recent schedule difficulty (games in last N days)."""
        start_date = (datetime.strptime(game_date, "%Y-%m-%d") -
                     timedelta(days=lookback)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            games = conn.execute("""
                SELECT COUNT(*) as game_count
                FROM games
                WHERE (home_team_abbrev = ? OR away_team_abbrev = ?)
                  AND game_date >= ? AND game_date < ?
                  AND home_score IS NOT NULL
            """, (team_abbrev, team_abbrev, start_date, game_date)).fetchone()

        game_count = games["game_count"] if games else 0

        # Typical is 3-4 games per week
        return {
            "games_last_week": game_count,
            "schedule_density": game_count / lookback,
            "is_heavy_schedule": game_count >= 4
        }

    def get_rest_advantage(self, home_team: str, away_team: str,
                           game_date: str) -> Dict[str, float]:
        """Calculate rest advantage between two teams with enhanced factors."""
        home_rest = self.get_rest_days(home_team, game_date)
        away_rest = self.get_rest_days(away_team, game_date)

        home_schedule = self.get_schedule_difficulty(home_team, game_date)
        away_schedule = self.get_schedule_difficulty(away_team, game_date)

        # Calculate rest advantage based on fatigue differential
        fatigue_diff = away_rest["fatigue_factor"] - home_rest["fatigue_factor"]

        # Back-to-back specific penalties (stronger effect)
        b2b_adjustment = 0.0

        # Home team on back-to-back vs rested away team
        if home_rest["is_back_to_back"] and away_rest["is_well_rested"]:
            b2b_adjustment = -0.08  # Strong disadvantage for home

        # Away team on back-to-back vs rested home team
        elif away_rest["is_back_to_back"] and home_rest["is_well_rested"]:
            b2b_adjustment = 0.08  # Strong advantage for home

        # Both on back-to-back (neutralizes)
        elif home_rest["is_back_to_back"] and away_rest["is_back_to_back"]:
            b2b_adjustment = 0.0

        # 3rd in 4 nights adjustments
        if home_rest["is_third_in_four"] and not away_rest["is_third_in_four"]:
            b2b_adjustment -= 0.05
        elif away_rest["is_third_in_four"] and not home_rest["is_third_in_four"]:
            b2b_adjustment += 0.05

        # Well-rested bonus
        well_rested_bonus = 0.0
        if home_rest["is_well_rested"] and not away_rest["is_well_rested"]:
            well_rested_bonus = 0.03
        elif away_rest["is_well_rested"] and not home_rest["is_well_rested"]:
            well_rested_bonus = -0.03

        # Total rest advantage
        rest_advantage = (fatigue_diff * 0.05) + b2b_adjustment + well_rested_bonus

        return {
            "home_rest_days": home_rest["rest_days"],
            "away_rest_days": away_rest["rest_days"],
            "rest_advantage": rest_advantage,
            "home_b2b": home_rest["is_back_to_back"],
            "away_b2b": away_rest["is_back_to_back"],
            "home_third_in_four": home_rest["is_third_in_four"],
            "away_third_in_four": away_rest["is_third_in_four"],
            "home_well_rested": home_rest["is_well_rested"],
            "away_well_rested": away_rest["is_well_rested"],
            "home_fatigue": home_rest["fatigue_factor"],
            "away_fatigue": away_rest["fatigue_factor"],
            "home_games_last_week": home_schedule["games_last_week"],
            "away_games_last_week": away_schedule["games_last_week"]
        }


class DisciplineAnalyzer:
    """
    Analyzes team discipline and shot blocking:
    - Penalty minutes taken (undisciplined teams give more power plays)
    - Blocked shots (defensive effort indicator)
    - Power play and penalty kill effectiveness
    """

    def __init__(self, db_path: str = "nhl_stats.db"):
        self.db_path = db_path
        self.team_discipline_cache: Dict[str, Dict] = {}

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_team_discipline_stats(self, team_abbrev: str, before_date: str,
                                   lookback_days: int = 60) -> Dict[str, Any]:
        """
        Get team discipline stats: PIM, blocked shots, special teams.
        """
        start_date = (datetime.strptime(before_date, "%Y-%m-%d") -
                     timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            # Get PIM and blocked shots from player_game_stats
            stats = conn.execute("""
                SELECT
                    SUM(pgs.pim) as total_pim,
                    SUM(pgs.blocked_shots) as total_blocks,
                    COUNT(DISTINCT pgs.game_id) as games
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.team_abbrev = ?
                  AND g.game_date >= ? AND g.game_date < ?
            """, (team_abbrev, start_date, before_date)).fetchone()

            # Get season special teams stats
            season_stats = conn.execute("""
                SELECT powerplay_pct, penalty_kill_pct, blocked_shots_per_game
                FROM team_season_stats
                WHERE team_abbrev = ?
                ORDER BY season DESC
                LIMIT 1
            """, (team_abbrev,)).fetchone()

        games = stats["games"] if stats and stats["games"] else 1
        total_pim = stats["total_pim"] if stats and stats["total_pim"] else 0
        total_blocks = stats["total_blocks"] if stats and stats["total_blocks"] else 0

        pim_per_game = total_pim / games
        blocks_per_game = total_blocks / games

        # Get special teams percentages
        pp_pct = season_stats["powerplay_pct"] if season_stats and season_stats["powerplay_pct"] else 20.0
        pk_pct = season_stats["penalty_kill_pct"] if season_stats and season_stats["penalty_kill_pct"] else 80.0

        # Calculate discipline rating (lower PIM = better discipline)
        # League avg PIM is ~8-10 per game
        # Rating: 0.5 = average, higher = more disciplined
        discipline_rating = 0.5 + (9.0 - pim_per_game) * 0.03
        discipline_rating = max(0.2, min(0.8, discipline_rating))

        # Block rating (higher blocks = better defensive effort)
        # League avg is ~14-16 blocks per game
        block_rating = 0.5 + (blocks_per_game - 15.0) * 0.015
        block_rating = max(0.2, min(0.8, block_rating))

        # Special teams rating
        # PP: league avg ~20%, PK: league avg ~80%
        pp_rating = 0.5 + (pp_pct - 20.0) * 0.015
        pk_rating = 0.5 + (pk_pct - 80.0) * 0.02
        special_teams_rating = (pp_rating + pk_rating) / 2

        return {
            "pim_per_game": round(pim_per_game, 2),
            "blocks_per_game": round(blocks_per_game, 2),
            "discipline_rating": round(discipline_rating, 3),
            "block_rating": round(block_rating, 3),
            "pp_pct": round(pp_pct, 1),
            "pk_pct": round(pk_pct, 1),
            "special_teams_rating": round(special_teams_rating, 3),
            "games_analyzed": games
        }

    def get_discipline_matchup(self, home_team: str, away_team: str,
                                game_date: str) -> Dict[str, Any]:
        """
        Calculate discipline and blocking matchup advantage.

        Key insights:
        - Undisciplined teams give opponents more power play opportunities
        - Teams that block more shots reduce opponent scoring chances
        - Good PK vs opponent's PP = advantage
        """
        home_stats = self.get_team_discipline_stats(home_team, game_date)
        away_stats = self.get_team_discipline_stats(away_team, game_date)

        # Discipline advantage: home team's discipline vs away team's
        # If away team takes more penalties, home gets more PP opportunities
        discipline_diff = home_stats["discipline_rating"] - away_stats["discipline_rating"]

        # Block advantage: teams that block more reduce opponent chances
        block_diff = home_stats["block_rating"] - away_stats["block_rating"]

        # Special teams matchup
        # Home PP vs Away PK, and Away PP vs Home PK
        home_pp_vs_away_pk = (home_stats["pp_pct"] / 100) * (1 - away_stats["pk_pct"] / 100)
        away_pp_vs_home_pk = (away_stats["pp_pct"] / 100) * (1 - home_stats["pk_pct"] / 100)
        special_teams_edge = home_pp_vs_away_pk - away_pp_vs_home_pk

        # Combined advantage
        # Discipline matters because undisciplined opponent = more PP chances
        # Blocks matter as defensive indicator
        combined_advantage = (
            discipline_diff * 0.3 +  # Discipline
            block_diff * 0.3 +       # Blocking
            special_teams_edge * 2.0 +  # Special teams matchup (scaled up)
            (home_stats["special_teams_rating"] - away_stats["special_teams_rating"]) * 0.4
        )

        return {
            "home_discipline_rating": home_stats["discipline_rating"],
            "away_discipline_rating": away_stats["discipline_rating"],
            "home_block_rating": home_stats["block_rating"],
            "away_block_rating": away_stats["block_rating"],
            "home_pim_per_game": home_stats["pim_per_game"],
            "away_pim_per_game": away_stats["pim_per_game"],
            "home_blocks_per_game": home_stats["blocks_per_game"],
            "away_blocks_per_game": away_stats["blocks_per_game"],
            "home_pp_pct": home_stats["pp_pct"],
            "away_pp_pct": away_stats["pp_pct"],
            "home_pk_pct": home_stats["pk_pct"],
            "away_pk_pct": away_stats["pk_pct"],
            "discipline_advantage": round(discipline_diff, 3),
            "block_advantage": round(block_diff, 3),
            "special_teams_edge": round(special_teams_edge, 4),
            "combined_discipline_advantage": round(combined_advantage, 3)
        }


class EnhancedMatchPredictor:
    """
    Enhanced match prediction model incorporating:
    - Rest days and fatigue
    - Goalie performance (INCREASED weight)
    - Player line chemistry
    - Team vs team matchups
    - Blocked shots and discipline (penalties)
    - Special teams (PP/PK)
    """

    def __init__(self, db_path: str = "nhl_stats.db", injury_tracker=None):
        self.db_path = db_path
        # Pass injury tracker to line analyzer to exclude injured players from chemistry calculations
        self.line_analyzer = LineChemistryAnalyzer(db_path, injury_tracker=injury_tracker)
        self.goalie_analyzer = GoalieAnalyzer(db_path)
        self.rest_analyzer = RestDaysAnalyzer(db_path)
        self.discipline_analyzer = DisciplineAnalyzer(db_path)
        self.injury_tracker = injury_tracker

        # Model weights (tuned for prediction)
        # Increased goalie weighting, added discipline/blocking
        self.weights = {
            "base_home_advantage": 0.04,  # ~4% home advantage
            "win_rate_diff": 0.14,        # Team quality (reduced)
            "goal_diff": 0.06,            # Scoring differential (reduced)
            "recent_form": 0.06,          # Last 5 games (reduced)
            "goalie": 0.16,               # Goalie performance (INCREASED from 0.12)
            "rest": 0.14,                 # Rest days (strong signal)
            "line_chemistry": 0.05,       # Offensive combinations
            "defense": 0.07,              # Defensive pairing strength
            "head_to_head": 0.04,         # H2H history
            "offense_vs_defense": 0.12,   # Bidirectional matchup
            "discipline": 0.06,           # NEW: Penalties/discipline
            "blocking": 0.06              # NEW: Shot blocking
        }

        self.team_stats: Dict[str, Dict] = {}
        self.trained = False

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def train(self, end_date: str, lookback_days: int = 60) -> Dict[str, Any]:
        """Train model on historical data."""
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") -
                     timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            games = conn.execute("""
                SELECT game_date, home_team_abbrev as home, away_team_abbrev as away,
                       home_score, away_score
                FROM games
                WHERE game_date >= ? AND game_date <= ?
                  AND home_score IS NOT NULL
                ORDER BY game_date
            """, (start_date, end_date)).fetchall()

        if len(games) < 20:
            return {"error": "Insufficient training data"}

        # Calculate team statistics
        team_data = defaultdict(lambda: {
            "wins": 0, "games": 0, "gf": 0, "ga": 0,
            "home_wins": 0, "home_games": 0,
            "away_wins": 0, "away_games": 0,
            "recent_results": []
        })

        for g in games:
            home_win = g["home_score"] > g["away_score"]

            # Home team
            team_data[g["home"]]["games"] += 1
            team_data[g["home"]]["home_games"] += 1
            team_data[g["home"]]["gf"] += g["home_score"]
            team_data[g["home"]]["ga"] += g["away_score"]
            if home_win:
                team_data[g["home"]]["wins"] += 1
                team_data[g["home"]]["home_wins"] += 1
            team_data[g["home"]]["recent_results"].append(1 if home_win else 0)

            # Away team
            team_data[g["away"]]["games"] += 1
            team_data[g["away"]]["away_games"] += 1
            team_data[g["away"]]["gf"] += g["away_score"]
            team_data[g["away"]]["ga"] += g["home_score"]
            if not home_win:
                team_data[g["away"]]["wins"] += 1
                team_data[g["away"]]["away_wins"] += 1
            team_data[g["away"]]["recent_results"].append(0 if home_win else 1)

        # Calculate rates
        for team, data in team_data.items():
            if data["games"] >= 5:
                self.team_stats[team] = {
                    "win_rate": data["wins"] / data["games"],
                    "home_win_rate": data["home_wins"] / data["home_games"] if data["home_games"] > 0 else 0.5,
                    "away_win_rate": data["away_wins"] / data["away_games"] if data["away_games"] > 0 else 0.5,
                    "goal_diff": (data["gf"] - data["ga"]) / data["games"],
                    "goals_for_avg": data["gf"] / data["games"],
                    "goals_against_avg": data["ga"] / data["games"],
                    "recent_form": sum(data["recent_results"][-5:]) / min(5, len(data["recent_results"]))
                }
            else:
                self.team_stats[team] = {
                    "win_rate": 0.5, "home_win_rate": 0.5, "away_win_rate": 0.5,
                    "goal_diff": 0, "goals_for_avg": 3.0, "goals_against_avg": 3.0,
                    "recent_form": 0.5
                }

        # Pre-analyze line chemistry for all teams
        for team in team_data.keys():
            self.line_analyzer.analyze_goal_combinations(team, end_date, lookback_days)

        self.trained = True

        # Calculate baseline home win rate
        home_wins = sum(1 for g in games if g["home_score"] > g["away_score"])
        home_win_rate = home_wins / len(games)

        return {
            "games_analyzed": len(games),
            "teams_trained": len(self.team_stats),
            "home_win_rate": home_win_rate
        }

    def get_head_to_head(self, home_team: str, away_team: str, before_date: str) -> Dict[str, float]:
        """Get head-to-head record."""
        start_date = (datetime.strptime(before_date, "%Y-%m-%d") -
                     timedelta(days=365)).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            games = conn.execute("""
                SELECT home_team_abbrev as home, home_score, away_score
                FROM games
                WHERE ((home_team_abbrev = ? AND away_team_abbrev = ?)
                    OR (home_team_abbrev = ? AND away_team_abbrev = ?))
                  AND game_date >= ? AND game_date < ?
                  AND home_score IS NOT NULL
            """, (home_team, away_team, away_team, home_team, start_date, before_date)).fetchall()

        if not games:
            return {"h2h_games": 0, "h2h_advantage": 0}

        home_wins = sum(1 for g in games if
                       (g["home"] == home_team and g["home_score"] > g["away_score"]) or
                       (g["home"] == away_team and g["away_score"] > g["home_score"]))

        return {
            "h2h_games": len(games),
            "h2h_advantage": (home_wins / len(games)) - 0.5  # -0.5 to 0.5 range
        }

    def predict(self, home_team: str, away_team: str, game_date: str) -> Dict[str, Any]:
        """Make a prediction for a specific game."""

        # Get team base stats
        home_stats = self.team_stats.get(home_team, {
            "win_rate": 0.5, "home_win_rate": 0.5, "goal_diff": 0, "recent_form": 0.5
        })
        away_stats = self.team_stats.get(away_team, {
            "win_rate": 0.5, "away_win_rate": 0.5, "goal_diff": 0, "recent_form": 0.5
        })

        # Start with base probability
        prob = 0.5

        # 1. Home advantage
        prob += self.weights["base_home_advantage"]

        # 2. Win rate difference
        win_rate_diff = home_stats["win_rate"] - away_stats["win_rate"]
        prob += win_rate_diff * self.weights["win_rate_diff"]

        # 3. Goal differential
        goal_diff_diff = home_stats["goal_diff"] - away_stats["goal_diff"]
        # Normalize to reasonable adjustment
        prob += min(0.1, max(-0.1, goal_diff_diff * 0.02)) * self.weights["goal_diff"] / 0.1

        # 4. Recent form
        form_diff = home_stats["recent_form"] - away_stats["recent_form"]
        prob += form_diff * self.weights["recent_form"]

        # 5. Goalie advantage
        goalie_matchup = self.goalie_analyzer.get_goalie_matchup_factor(home_team, away_team, game_date)
        prob += goalie_matchup["goalie_advantage"] * self.weights["goalie"]

        # 6. Rest advantage (ENHANCED - strongest signal)
        rest_matchup = self.rest_analyzer.get_rest_advantage(home_team, away_team, game_date)
        # Rest advantage is already calculated with proper scaling
        prob += rest_matchup["rest_advantage"] * (self.weights["rest"] / 0.08)  # Scaled to match impact

        # 7. Bidirectional matchup analysis (offense vs opponent defense)
        # This analyzes: home offense vs away D AND away offense vs home D
        bidirectional_matchup = self.line_analyzer.get_bidirectional_matchup(home_team, away_team, game_date)
        matchup_advantage = bidirectional_matchup["combined_matchup_advantage"]
        prob += matchup_advantage * self.weights["offense_vs_defense"]

        # 8. Line chemistry (general offensive combinations)
        home_line_strength = bidirectional_matchup["home_line_strength"]
        away_line_strength = bidirectional_matchup["away_line_strength"]
        line_diff = home_line_strength - away_line_strength
        prob += line_diff * self.weights["line_chemistry"]

        # 9. Defensive pairing strength
        home_defense = bidirectional_matchup["home_defense_rating"]
        away_defense = bidirectional_matchup["away_defense_rating"]
        defense_diff = home_defense - away_defense
        prob += defense_diff * self.weights["defense"]

        # 10. Head-to-head record
        h2h = self.get_head_to_head(home_team, away_team, game_date)
        if h2h["h2h_games"] >= 2:
            prob += h2h["h2h_advantage"] * self.weights["head_to_head"]

        # 11. Discipline and blocking analysis (NEW)
        discipline_matchup = self.discipline_analyzer.get_discipline_matchup(home_team, away_team, game_date)
        discipline_adv = discipline_matchup["discipline_advantage"]
        block_adv = discipline_matchup["block_advantage"]
        special_teams_edge = discipline_matchup["special_teams_edge"]

        # Apply discipline advantage (disciplined team gets fewer penalties against)
        prob += discipline_adv * self.weights["discipline"]

        # Apply blocking advantage (teams that block more reduce opponent chances)
        prob += block_adv * self.weights["blocking"]

        # Add special teams edge as additional factor
        prob += special_teams_edge * 1.5  # Special teams already scaled in analyzer

        # Clip probability to reasonable range
        prob = max(0.25, min(0.75, prob))

        # Calculate confidence based on signal strength
        signals = [
            abs(win_rate_diff),
            abs(goal_diff_diff) / 2,
            abs(form_diff),
            abs(goalie_matchup["goalie_advantage"]) * 3,
            abs(line_diff),
            abs(defense_diff),
            abs(rest_matchup["rest_advantage"]) * 5,  # Rest is important signal
            abs(matchup_advantage) * 4,  # Offense vs defense matchup
            abs(discipline_adv) * 3,  # Discipline
            abs(block_adv) * 3  # Blocking
        ]
        signal_strength = statistics.mean(signals)
        confidence = min(1.0, signal_strength * 2)

        predicted_winner = home_team if prob >= 0.5 else away_team

        # Extract H2H specific data for analysis
        home_off_vs_away_def = bidirectional_matchup["home_offense_vs_away_defense"]
        away_off_vs_home_def = bidirectional_matchup["away_offense_vs_home_defense"]

        return {
            "home_team": home_team,
            "away_team": away_team,
            "game_date": game_date,
            "home_win_prob": round(prob, 3),
            "away_win_prob": round(1 - prob, 3),
            "predicted_winner": predicted_winner,
            "confidence": round(confidence, 3),
            "factors": {
                "home_win_rate": round(home_stats["win_rate"], 3),
                "away_win_rate": round(away_stats["win_rate"], 3),
                "home_recent_form": round(home_stats["recent_form"], 3),
                "away_recent_form": round(away_stats["recent_form"], 3),
                "goalie_advantage": round(goalie_matchup["goalie_advantage"], 3),
                "rest_advantage": round(rest_matchup["rest_advantage"], 3),
                "home_line_strength": round(home_line_strength, 3),
                "away_line_strength": round(away_line_strength, 3),
                "home_defense_rating": round(home_defense, 3),
                "away_defense_rating": round(away_defense, 3),
                "matchup_advantage": round(matchup_advantage, 3),
                "h2h_matchup_advantage": round(bidirectional_matchup["h2h_matchup_advantage"], 3),
                "style_matchup_advantage": round(bidirectional_matchup["style_matchup_advantage"], 3),
                "home_off_vs_away_def_rating": home_off_vs_away_def["offensive_rating_vs_opponent"],
                "away_off_vs_home_def_rating": away_off_vs_home_def["offensive_rating_vs_opponent"],
                "has_h2h_matchup_data": bidirectional_matchup["has_h2h_data"],
                "home_rest_days": rest_matchup["home_rest_days"],
                "away_rest_days": rest_matchup["away_rest_days"],
                "home_b2b": rest_matchup["home_b2b"],
                "away_b2b": rest_matchup["away_b2b"],
                "home_well_rested": rest_matchup.get("home_well_rested", False),
                "away_well_rested": rest_matchup.get("away_well_rested", False),
                "home_third_in_four": rest_matchup.get("home_third_in_four", False),
                "away_third_in_four": rest_matchup.get("away_third_in_four", False),
                # NEW: Discipline and blocking factors
                "discipline_advantage": round(discipline_adv, 3),
                "block_advantage": round(block_adv, 3),
                "home_pim_per_game": discipline_matchup["home_pim_per_game"],
                "away_pim_per_game": discipline_matchup["away_pim_per_game"],
                "home_blocks_per_game": discipline_matchup["home_blocks_per_game"],
                "away_blocks_per_game": discipline_matchup["away_blocks_per_game"],
                "home_pp_pct": discipline_matchup["home_pp_pct"],
                "away_pp_pct": discipline_matchup["away_pp_pct"],
                "home_pk_pct": discipline_matchup["home_pk_pct"],
                "away_pk_pct": discipline_matchup["away_pk_pct"],
                "special_teams_edge": round(special_teams_edge, 4)
            }
        }


class EnhancedBacktester:
    """Backtester for enhanced prediction model."""

    def __init__(self, db_path: str = "nhl_stats.db"):
        self.db_path = db_path

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_date_range(self) -> Tuple[str, str]:
        """Get available date range."""
        with self.get_connection() as conn:
            row = conn.execute("""
                SELECT MIN(game_date) as min_date, MAX(game_date) as max_date
                FROM games WHERE home_score IS NOT NULL
            """).fetchone()
        return row["min_date"], row["max_date"]

    def backtest(self, test_start: str, test_end: str) -> Dict[str, Any]:
        """Run backtest on date range."""
        print(f"Backtest period: {test_start} to {test_end}")

        with self.get_connection() as conn:
            test_games = conn.execute("""
                SELECT game_id, game_date, home_team_abbrev as home,
                       away_team_abbrev as away, home_score, away_score
                FROM games
                WHERE game_date >= ? AND game_date <= ?
                  AND home_score IS NOT NULL
                ORDER BY game_date
            """, (test_start, test_end)).fetchall()

        print(f"Test games: {len(test_games)}")

        if not test_games:
            return {"error": "No test games found"}

        results = []
        predictor = EnhancedMatchPredictor(self.db_path)
        current_train_date = None

        # Track metrics
        correct = 0
        by_confidence = defaultdict(lambda: {"correct": 0, "total": 0})
        by_rest_advantage = {"favored_rested": {"correct": 0, "total": 0},
                            "favored_tired": {"correct": 0, "total": 0},
                            "neutral": {"correct": 0, "total": 0}}
        by_b2b = {"home_b2b": {"correct": 0, "total": 0},
                  "away_b2b": {"correct": 0, "total": 0},
                  "both_b2b": {"correct": 0, "total": 0},
                  "neither_b2b": {"correct": 0, "total": 0}}
        by_defense = {"home_defense_edge": {"correct": 0, "total": 0},
                      "away_defense_edge": {"correct": 0, "total": 0},
                      "defense_neutral": {"correct": 0, "total": 0}}
        by_goalie = {"goalie_edge": {"correct": 0, "total": 0},
                     "no_edge": {"correct": 0, "total": 0}}
        by_matchup = {"home_matchup_edge": {"correct": 0, "total": 0},
                      "away_matchup_edge": {"correct": 0, "total": 0},
                      "matchup_neutral": {"correct": 0, "total": 0}}
        by_h2h_data = {"has_h2h_data": {"correct": 0, "total": 0},
                       "no_h2h_data": {"correct": 0, "total": 0}}
        by_discipline = {"home_disciplined": {"correct": 0, "total": 0},
                         "away_disciplined": {"correct": 0, "total": 0},
                         "discipline_neutral": {"correct": 0, "total": 0}}
        by_blocking = {"home_blocks_more": {"correct": 0, "total": 0},
                       "away_blocks_more": {"correct": 0, "total": 0},
                       "blocking_neutral": {"correct": 0, "total": 0}}

        for i, game in enumerate(test_games):
            # Retrain model for each new date
            train_date = (datetime.strptime(game["game_date"], "%Y-%m-%d") -
                         timedelta(days=1)).strftime("%Y-%m-%d")

            if train_date != current_train_date:
                predictor.train(train_date, lookback_days=60)
                current_train_date = train_date
                if i % 50 == 0:
                    print(f"  Processing game {i+1}/{len(test_games)}...")

            # Make prediction
            pred = predictor.predict(game["home"], game["away"], game["game_date"])

            # Determine actual result
            actual_home_win = game["home_score"] > game["away_score"]
            actual_winner = game["home"] if actual_home_win else game["away"]
            is_correct = pred["predicted_winner"] == actual_winner

            if is_correct:
                correct += 1

            # Track by confidence bucket
            conf_bucket = round(pred["confidence"], 1)
            by_confidence[conf_bucket]["total"] += 1
            if is_correct:
                by_confidence[conf_bucket]["correct"] += 1

            # Track by rest advantage
            rest_adv = pred["factors"]["rest_advantage"]
            if rest_adv > 0.02:
                by_rest_advantage["favored_rested"]["total"] += 1
                if is_correct:
                    by_rest_advantage["favored_rested"]["correct"] += 1
            elif rest_adv < -0.02:
                by_rest_advantage["favored_tired"]["total"] += 1
                if is_correct:
                    by_rest_advantage["favored_tired"]["correct"] += 1
            else:
                by_rest_advantage["neutral"]["total"] += 1
                if is_correct:
                    by_rest_advantage["neutral"]["correct"] += 1

            # Track by goalie advantage
            goalie_adv = abs(pred["factors"]["goalie_advantage"])
            if goalie_adv > 0.01:
                by_goalie["goalie_edge"]["total"] += 1
                if is_correct:
                    by_goalie["goalie_edge"]["correct"] += 1
            else:
                by_goalie["no_edge"]["total"] += 1
                if is_correct:
                    by_goalie["no_edge"]["correct"] += 1

            # Track by back-to-back status
            home_b2b = pred["factors"].get("home_b2b", False)
            away_b2b = pred["factors"].get("away_b2b", False)
            if home_b2b and away_b2b:
                by_b2b["both_b2b"]["total"] += 1
                if is_correct:
                    by_b2b["both_b2b"]["correct"] += 1
            elif home_b2b:
                by_b2b["home_b2b"]["total"] += 1
                if is_correct:
                    by_b2b["home_b2b"]["correct"] += 1
            elif away_b2b:
                by_b2b["away_b2b"]["total"] += 1
                if is_correct:
                    by_b2b["away_b2b"]["correct"] += 1
            else:
                by_b2b["neither_b2b"]["total"] += 1
                if is_correct:
                    by_b2b["neither_b2b"]["correct"] += 1

            # Track by defense rating differential
            home_def = pred["factors"].get("home_defense_rating", 0.5)
            away_def = pred["factors"].get("away_defense_rating", 0.5)
            def_diff = home_def - away_def
            if def_diff > 0.05:
                by_defense["home_defense_edge"]["total"] += 1
                if is_correct:
                    by_defense["home_defense_edge"]["correct"] += 1
            elif def_diff < -0.05:
                by_defense["away_defense_edge"]["total"] += 1
                if is_correct:
                    by_defense["away_defense_edge"]["correct"] += 1
            else:
                by_defense["defense_neutral"]["total"] += 1
                if is_correct:
                    by_defense["defense_neutral"]["correct"] += 1

            # Track by offense vs defense matchup advantage
            matchup_adv = pred["factors"].get("matchup_advantage", 0)
            if matchup_adv > 0.03:
                by_matchup["home_matchup_edge"]["total"] += 1
                if is_correct:
                    by_matchup["home_matchup_edge"]["correct"] += 1
            elif matchup_adv < -0.03:
                by_matchup["away_matchup_edge"]["total"] += 1
                if is_correct:
                    by_matchup["away_matchup_edge"]["correct"] += 1
            else:
                by_matchup["matchup_neutral"]["total"] += 1
                if is_correct:
                    by_matchup["matchup_neutral"]["correct"] += 1

            # Track by H2H data availability
            has_h2h = pred["factors"].get("has_h2h_matchup_data", False)
            if has_h2h:
                by_h2h_data["has_h2h_data"]["total"] += 1
                if is_correct:
                    by_h2h_data["has_h2h_data"]["correct"] += 1
            else:
                by_h2h_data["no_h2h_data"]["total"] += 1
                if is_correct:
                    by_h2h_data["no_h2h_data"]["correct"] += 1

            # Track by discipline advantage (NEW)
            disc_adv = pred["factors"].get("discipline_advantage", 0)
            if disc_adv > 0.02:
                by_discipline["home_disciplined"]["total"] += 1
                if is_correct:
                    by_discipline["home_disciplined"]["correct"] += 1
            elif disc_adv < -0.02:
                by_discipline["away_disciplined"]["total"] += 1
                if is_correct:
                    by_discipline["away_disciplined"]["correct"] += 1
            else:
                by_discipline["discipline_neutral"]["total"] += 1
                if is_correct:
                    by_discipline["discipline_neutral"]["correct"] += 1

            # Track by blocking advantage (NEW)
            block_adv = pred["factors"].get("block_advantage", 0)
            if block_adv > 0.02:
                by_blocking["home_blocks_more"]["total"] += 1
                if is_correct:
                    by_blocking["home_blocks_more"]["correct"] += 1
            elif block_adv < -0.02:
                by_blocking["away_blocks_more"]["total"] += 1
                if is_correct:
                    by_blocking["away_blocks_more"]["correct"] += 1
            else:
                by_blocking["blocking_neutral"]["total"] += 1
                if is_correct:
                    by_blocking["blocking_neutral"]["correct"] += 1

            results.append({
                "game_id": game["game_id"],
                "date": game["game_date"],
                "home_team": game["home"],
                "away_team": game["away"],
                "predicted_winner": pred["predicted_winner"],
                "actual_winner": actual_winner,
                "home_win_prob": pred["home_win_prob"],
                "confidence": pred["confidence"],
                "correct": is_correct,
                "factors": pred["factors"]
            })

        # Calculate overall accuracy
        accuracy = correct / len(results)

        # Calculate accuracy by confidence
        confidence_accuracy = {}
        for conf, data in sorted(by_confidence.items()):
            if data["total"] > 0:
                confidence_accuracy[conf] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by rest
        rest_accuracy = {}
        for category, data in by_rest_advantage.items():
            if data["total"] > 0:
                rest_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by goalie
        goalie_accuracy = {}
        for category, data in by_goalie.items():
            if data["total"] > 0:
                goalie_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by back-to-back
        b2b_accuracy = {}
        for category, data in by_b2b.items():
            if data["total"] > 0:
                b2b_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by defense
        defense_accuracy = {}
        for category, data in by_defense.items():
            if data["total"] > 0:
                defense_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by matchup advantage
        matchup_accuracy = {}
        for category, data in by_matchup.items():
            if data["total"] > 0:
                matchup_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by H2H data availability
        h2h_data_accuracy = {}
        for category, data in by_h2h_data.items():
            if data["total"] > 0:
                h2h_data_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by discipline (NEW)
        discipline_accuracy = {}
        for category, data in by_discipline.items():
            if data["total"] > 0:
                discipline_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        # Calculate accuracy by blocking (NEW)
        blocking_accuracy = {}
        for category, data in by_blocking.items():
            if data["total"] > 0:
                blocking_accuracy[category] = {
                    "accuracy": data["correct"] / data["total"],
                    "games": data["total"]
                }

        return {
            "test_period": f"{test_start} to {test_end}",
            "total_games": len(results),
            "correct_predictions": correct,
            "accuracy": round(accuracy, 4),
            "by_confidence": confidence_accuracy,
            "by_rest": rest_accuracy,
            "by_b2b": b2b_accuracy,
            "by_defense": defense_accuracy,
            "by_goalie": goalie_accuracy,
            "by_matchup": matchup_accuracy,
            "by_h2h_data": h2h_data_accuracy,
            "by_discipline": discipline_accuracy,
            "by_blocking": blocking_accuracy,
            "predictions": results
        }

    def run_backtest(self, weeks: int = 8) -> Dict[str, Any]:
        """Run backtest for specified weeks."""
        min_date, max_date = self.get_date_range()
        print(f"Data available: {min_date} to {max_date}")

        max_dt = datetime.strptime(max_date, "%Y-%m-%d")
        test_start_dt = max_dt - timedelta(weeks=weeks)
        test_start = test_start_dt.strftime("%Y-%m-%d")

        print(f"\nRunning {weeks}-week backtest...")
        print("=" * 60)

        return self.backtest(test_start, max_date)


def print_results(results: Dict[str, Any]) -> None:
    """Print formatted backtest results."""
    print("\n" + "=" * 70)
    print("ENHANCED MODEL BACKTEST RESULTS")
    print("=" * 70)
    print(f"Period: {results['test_period']}")
    print(f"Total Games: {results['total_games']}")
    print(f"Correct Predictions: {results['correct_predictions']}")
    print(f"\n*** OVERALL ACCURACY: {results['accuracy']:.1%} ***")

    print("\n--- Accuracy by Confidence Level ---")
    for conf, data in results.get("by_confidence", {}).items():
        print(f"  Confidence {conf:.1f}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Rest Advantage ---")
    for category, data in results.get("by_rest", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Back-to-Back Status ---")
    for category, data in results.get("by_b2b", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Defensive Edge ---")
    for category, data in results.get("by_defense", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Goalie Edge ---")
    for category, data in results.get("by_goalie", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Offense vs Defense Matchup ---")
    for category, data in results.get("by_matchup", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by H2H Data Availability ---")
    for category, data in results.get("by_h2h_data", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Discipline (Penalties) ---")
    for category, data in results.get("by_discipline", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    print("\n--- Accuracy by Shot Blocking ---")
    for category, data in results.get("by_blocking", {}).items():
        label = category.replace("_", " ").title()
        print(f"  {label}: {data['accuracy']:.1%} ({data['games']} games)")

    # Show some sample predictions
    print("\n--- Sample Predictions (last 10 games) ---")
    for pred in results.get("predictions", [])[-10:]:
        status = "OK" if pred["correct"] else "XX"
        print(f"  [{status}] {pred['date']}: {pred['away_team']} @ {pred['home_team']} "
              f"| Predicted: {pred['predicted_winner']} ({pred['home_win_prob']:.0%}) "
              f"| Actual: {pred['actual_winner']}")

    print("\n" + "=" * 70)


def run_enhanced_backtest():
    """Run the enhanced model backtest."""
    backtester = EnhancedBacktester()
    results = backtester.run_backtest(weeks=8)
    print_results(results)
    return results


class EnhancedPlayerPredictor:
    """
    Enhanced player prediction model that predicts:
    - Shots on goal with probability thresholds and confidence
    - Blocked shots with probability thresholds and confidence
    - Goals, assists, points (existing)

    Integrates ALL game-level factors from EnhancedMatchPredictor:
    - Rest days / back-to-back status
    - Goalie quality (opponent goalie affects shots)
    - Line chemistry
    - Discipline / special teams
    - Pace of play (team shot volume tendencies)

    Uses position-specific modeling since D-men block more but shoot less.
    """

    def __init__(self, db_path: str = "nhl_stats.db", use_injuries: bool = True):
        self.db_path = db_path
        # Position-specific baseline stats (from data analysis)
        self.position_baselines = {
            "C": {"shots": 1.65, "blocked": 0.54},
            "L": {"shots": 1.75, "blocked": 0.47},
            "R": {"shots": 1.82, "blocked": 0.47},
            "D": {"shots": 1.32, "blocked": 1.38}
        }
        self.player_cache: Dict[int, Dict] = {}
        self.opponent_factors: Dict[str, Dict] = {}
        self.game_context_cache: Dict[str, Dict] = {}
        self.trained = False

        # Initialize injury tracker (optional) - do this first so we can pass to analyzers
        self.injury_tracker = None
        self.use_injuries = use_injuries
        if use_injuries:
            try:
                from injury_tracker import InjuryTracker
                self.injury_tracker = InjuryTracker()
            except ImportError:
                pass  # Injury tracker not available

        # Initialize the game-level analyzers (same as EnhancedMatchPredictor)
        self.rest_analyzer = RestDaysAnalyzer(db_path)
        self.goalie_analyzer = GoalieAnalyzer(db_path)
        # Pass injury tracker to line analyzer so it can exclude injured players
        # from chemistry and defense calculations
        self.line_analyzer = LineChemistryAnalyzer(db_path, injury_tracker=self.injury_tracker)
        self.discipline_analyzer = DisciplineAnalyzer(db_path)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_player_historical_stats(self, player_id: int, before_date: str,
                                     lookback_games: int = 15) -> Dict[str, Any]:
        """
        Get player's historical stats for prediction baseline.
        Returns rolling averages for shots and blocked shots.
        """
        if player_id in self.player_cache:
            cached = self.player_cache[player_id]
            if cached.get("calculated_date") == before_date:
                return cached

        with self.get_connection() as conn:
            # Get player info
            player = conn.execute("""
                SELECT player_id, full_name, position, team_abbrev
                FROM players
                WHERE player_id = ?
            """, (player_id,)).fetchone()

            if not player:
                return self._empty_player_stats()

            # Get recent game stats
            games = conn.execute("""
                SELECT
                    pgs.shots,
                    pgs.blocked_shots,
                    pgs.goals,
                    pgs.assists,
                    pgs.points,
                    pgs.toi_seconds,
                    pgs.hits,
                    g.game_date,
                    pgs.opponent_abbrev,
                    pgs.is_home
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.player_id = ?
                  AND g.game_date < ?
                ORDER BY g.game_date DESC
                LIMIT ?
            """, (player_id, before_date, lookback_games)).fetchall()

        if not games:
            position = player["position"] if player else "C"
            baseline = self.position_baselines.get(position, {"shots": 1.5, "blocked": 0.7})
            return {
                "player_id": player_id,
                "player_name": player["full_name"] if player else "Unknown",
                "position": position,
                "team": player["team_abbrev"] if player else "",
                "games_analyzed": 0,
                "avg_shots": baseline["shots"],
                "avg_blocked": baseline["blocked"],
                "avg_goals": 0.2,
                "avg_assists": 0.3,
                "avg_points": 0.5,
                "avg_toi": 900,  # 15 min default
                "shots_std": 1.0,
                "blocked_std": 0.8,
                "recent_5_shots": baseline["shots"],
                "recent_5_blocked": baseline["blocked"],
                "calculated_date": before_date
            }

        # Calculate statistics
        shots = [g["shots"] or 0 for g in games]
        blocked = [g["blocked_shots"] or 0 for g in games]
        goals = [g["goals"] or 0 for g in games]
        assists = [g["assists"] or 0 for g in games]
        points = [g["points"] or 0 for g in games]
        toi = [g["toi_seconds"] or 0 for g in games]

        # Recent form (last 5 games)
        recent_shots = shots[:5] if len(shots) >= 5 else shots
        recent_blocked = blocked[:5] if len(blocked) >= 5 else blocked

        # Calculate TOI volatility for uncertainty adjustment
        toi_std = statistics.stdev(toi) if len(toi) > 1 else 0
        avg_toi_val = statistics.mean(toi) if toi else 900
        toi_cv = (toi_std / avg_toi_val) if avg_toi_val > 0 else 0  # Coefficient of variation

        # Breakout detection: is player shooting significantly above season average recently?
        avg_shots_val = statistics.mean(shots) if shots else 1.5
        recent_shots_val = statistics.mean(recent_shots) if recent_shots else avg_shots_val
        breakout_threshold = avg_shots_val + 1.0  # 1 SOG above season average
        is_breakout = recent_shots_val >= breakout_threshold and len(recent_shots) >= 3
        breakout_factor = min(1.15, recent_shots_val / avg_shots_val) if avg_shots_val > 0 else 1.0

        stats = {
            "player_id": player_id,
            "player_name": player["full_name"] if player else "Unknown",
            "position": player["position"] if player else "C",
            "team": player["team_abbrev"] if player else "",
            "games_analyzed": len(games),
            "avg_shots": avg_shots_val,
            "avg_blocked": statistics.mean(blocked) if blocked else 0.7,
            "avg_goals": statistics.mean(goals) if goals else 0.2,
            "avg_assists": statistics.mean(assists) if assists else 0.3,
            "avg_points": statistics.mean(points) if points else 0.5,
            "avg_toi": avg_toi_val,
            "shots_std": statistics.stdev(shots) if len(shots) > 1 else 1.0,
            "blocked_std": statistics.stdev(blocked) if len(blocked) > 1 else 0.8,
            "recent_5_shots": recent_shots_val,
            "recent_5_blocked": statistics.mean(recent_blocked) if recent_blocked else 0.7,
            "max_shots": max(shots) if shots else 0,
            "max_blocked": max(blocked) if blocked else 0,
            "calculated_date": before_date,
            # NEW: TOI volatility tracking
            "toi_std": toi_std,
            "toi_cv": toi_cv,  # Coefficient of variation (higher = more volatile)
            # NEW: Breakout detection
            "is_breakout": is_breakout,
            "breakout_factor": breakout_factor if is_breakout else 1.0,
        }

        self.player_cache[player_id] = stats
        return stats

    def _empty_player_stats(self) -> Dict[str, Any]:
        """Return empty stats for unknown player."""
        return {
            "player_id": 0,
            "player_name": "Unknown",
            "position": "C",
            "team": "",
            "games_analyzed": 0,
            "avg_shots": 1.5,
            "avg_blocked": 0.7,
            "avg_goals": 0.2,
            "avg_assists": 0.3,
            "avg_points": 0.5,
            "avg_toi": 900,
            "shots_std": 1.0,
            "blocked_std": 0.8,
            "recent_5_shots": 1.5,
            "recent_5_blocked": 0.7,
            "toi_std": 0,
            "toi_cv": 0,
            "is_breakout": False,
            "breakout_factor": 1.0,
        }

    def get_opponent_defensive_factor(self, opponent: str, before_date: str) -> Dict[str, float]:
        """
        Calculate how opponent affects shots and blocks.
        Teams that allow more shots = higher shots factor.
        Teams that block more = lower opponent shot opportunities.
        """
        cache_key = f"{opponent}_{before_date}"
        if cache_key in self.opponent_factors:
            return self.opponent_factors[cache_key]

        with self.get_connection() as conn:
            # Get shots allowed by opponent (from player stats against them)
            stats = conn.execute("""
                SELECT
                    AVG(pgs.shots) as avg_shots_allowed,
                    AVG(pgs.blocked_shots) as avg_blocks_vs
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.opponent_abbrev = ?
                  AND g.game_date < ?
                  AND g.game_date >= date(?, '-60 days')
            """, (opponent, before_date, before_date)).fetchone()

            # Get opponent's blocked shots (they block against attacker)
            opp_blocks = conn.execute("""
                SELECT AVG(pgs.blocked_shots) as team_blocks
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.team_abbrev = ?
                  AND g.game_date < ?
                  AND g.game_date >= date(?, '-60 days')
            """, (opponent, before_date, before_date)).fetchone()

        # League averages for normalization
        league_avg_shots = 1.54
        league_avg_blocks = 0.79

        avg_shots_allowed = stats["avg_shots_allowed"] if stats and stats["avg_shots_allowed"] else league_avg_shots
        opp_team_blocks = opp_blocks["team_blocks"] if opp_blocks and opp_blocks["team_blocks"] else league_avg_blocks

        # Factor > 1 means opponent allows more shots than average
        shots_factor = avg_shots_allowed / league_avg_shots if league_avg_shots > 0 else 1.0
        # Factor > 1 means more blocking opportunities (opponent blocks more)
        blocks_factor = opp_team_blocks / league_avg_blocks if league_avg_blocks > 0 else 1.0

        factors = {
            "shots_factor": min(1.3, max(0.7, shots_factor)),
            "blocks_factor": min(1.3, max(0.7, blocks_factor))
        }

        self.opponent_factors[cache_key] = factors
        return factors

    def get_game_context(self, player_team: str, opponent: str, game_date: str,
                          is_home: bool) -> Dict[str, Any]:
        """
        Get all game-level context factors that affect player predictions.
        Uses the same analyzers as EnhancedMatchPredictor.
        """
        cache_key = f"{player_team}_{opponent}_{game_date}_{is_home}"
        if cache_key in self.game_context_cache:
            return self.game_context_cache[cache_key]

        # 1. REST/FATIGUE FACTORS
        rest_info = self.rest_analyzer.get_rest_days(player_team, game_date)
        opp_rest_info = self.rest_analyzer.get_rest_days(opponent, game_date)

        # Fatigue reduces shots (tired players shoot less)
        # Back-to-back = 0.92x shots, 3rd in 4 nights = 0.95x
        if rest_info["is_back_to_back"]:
            fatigue_shot_factor = 0.92
        elif rest_info["is_third_in_four"]:
            fatigue_shot_factor = 0.95
        elif rest_info["is_well_rested"]:
            fatigue_shot_factor = 1.03  # Well rested = more energy
        else:
            fatigue_shot_factor = 1.0

        # Fatigue increases blocking (tired teams play more defensive)
        if rest_info["is_back_to_back"]:
            fatigue_block_factor = 1.05
        else:
            fatigue_block_factor = 1.0

        # 2. OPPONENT GOALIE QUALITY
        # Weaker goalie = more shots (players shoot more when they sense opportunity)
        goalie_matchup = self.goalie_analyzer.get_goalie_matchup_factor(
            player_team if is_home else opponent,
            opponent if is_home else player_team,
            game_date
        )

        # If opponent has weak goalie, players take more shots
        opp_goalie_sv_pct = goalie_matchup["away_goalie_save_pct"] if is_home else goalie_matchup["home_goalie_save_pct"]
        # League average is ~0.905, scale shot increase for weaker goalies
        goalie_shot_factor = 1.0 + (0.905 - opp_goalie_sv_pct) * 2  # Each 1% below avg = 2% more shots
        goalie_shot_factor = max(0.9, min(1.15, goalie_shot_factor))

        # 3. SPECIAL TEAMS / DISCIPLINE
        discipline_matchup = self.discipline_analyzer.get_discipline_matchup(
            player_team if is_home else opponent,
            opponent if is_home else player_team,
            game_date
        )

        # Undisciplined opponent = more power play time = more shots for PP players
        opp_pim = discipline_matchup["away_pim_per_game"] if is_home else discipline_matchup["home_pim_per_game"]
        # League avg PIM ~8-10, high PIM opponent = more PP opportunities
        pp_opportunity_factor = 1.0 + (opp_pim - 9.0) * 0.01
        pp_opportunity_factor = max(0.95, min(1.10, pp_opportunity_factor))

        # 4. PACE OF PLAY (team shot volume)
        with self.get_connection() as conn:
            # Get team's average shots per game
            team_pace = conn.execute("""
                SELECT AVG(total_shots) as avg_shots
                FROM (
                    SELECT g.game_id, SUM(pgs.shots) as total_shots
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    WHERE pgs.team_abbrev = ?
                      AND g.game_date < ?
                      AND g.game_date >= date(?, '-60 days')
                    GROUP BY g.game_id
                )
            """, (player_team, game_date, game_date)).fetchone()

            # Get opponent's shots allowed per game
            opp_shots_allowed = conn.execute("""
                SELECT AVG(total_shots) as avg_shots_allowed
                FROM (
                    SELECT g.game_id, SUM(pgs.shots) as total_shots
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    WHERE pgs.opponent_abbrev = ?
                      AND g.game_date < ?
                      AND g.game_date >= date(?, '-60 days')
                    GROUP BY g.game_id
                )
            """, (opponent, game_date, game_date)).fetchone()

        # League average ~30 shots per team per game
        league_avg_team_shots = 30.0
        team_shot_pace = team_pace["avg_shots"] if team_pace and team_pace["avg_shots"] else league_avg_team_shots
        opp_allows = opp_shots_allowed["avg_shots_allowed"] if opp_shots_allowed and opp_shots_allowed["avg_shots_allowed"] else league_avg_team_shots

        # Combined pace factor with INCREASED SENSITIVITY for high-scoring matchups
        # Base pace from team shot volume and opponent allowance
        team_pace_ratio = team_shot_pace / league_avg_team_shots
        opp_allows_ratio = opp_allows / league_avg_team_shots

        # Increase sensitivity: both high-volume team AND porous opponent = multiplicative boost
        if team_pace_ratio > 1.05 and opp_allows_ratio > 1.05:
            # High-scoring matchup: apply extra boost
            pace_factor = (team_pace_ratio + opp_allows_ratio) / 2
            pace_factor *= 1.05  # Additional 5% boost for high-scoring matchups
        elif team_pace_ratio < 0.95 and opp_allows_ratio < 0.95:
            # Low-scoring matchup: apply extra reduction
            pace_factor = (team_pace_ratio + opp_allows_ratio) / 2
            pace_factor *= 0.97  # Additional 3% reduction for low-scoring matchups
        else:
            # Normal matchup
            pace_factor = (team_pace_ratio + opp_allows_ratio) / 2

        # Wider range to allow more impact: 0.82 to 1.22
        pace_factor = max(0.82, min(1.22, pace_factor))

        # 5. LINE CHEMISTRY (for offensive players)
        line_strength = self.line_analyzer.get_team_line_strength(player_team, game_date)
        # Good line chemistry = slightly more shots (better puck movement)
        chemistry_factor = 0.95 + (line_strength * 0.10)  # 0.95 to 1.05

        # 6. DEFENSE MATCHUP (affects blocking)
        defense_rating = self.line_analyzer.get_defense_rating(player_team, game_date)
        opp_offense_strength = self.line_analyzer.get_team_line_strength(opponent, game_date)
        # Strong opponent offense = more blocking needed
        defense_block_factor = 1.0 + (opp_offense_strength - 0.5) * 0.15
        defense_block_factor = max(0.9, min(1.15, defense_block_factor))

        context = {
            # Rest factors
            "is_back_to_back": rest_info["is_back_to_back"],
            "is_third_in_four": rest_info["is_third_in_four"],
            "is_well_rested": rest_info["is_well_rested"],
            "rest_days": rest_info["rest_days"],
            "fatigue_shot_factor": fatigue_shot_factor,
            "fatigue_block_factor": fatigue_block_factor,

            # Goalie factors
            "opponent_goalie_sv_pct": opp_goalie_sv_pct,
            "goalie_shot_factor": goalie_shot_factor,

            # Special teams
            "opponent_pim_per_game": opp_pim,
            "pp_opportunity_factor": pp_opportunity_factor,

            # Pace
            "team_shot_pace": team_shot_pace,
            "opponent_shots_allowed": opp_allows,
            "pace_factor": pace_factor,

            # Chemistry
            "line_chemistry_strength": line_strength,
            "chemistry_factor": chemistry_factor,

            # Defense/blocking
            "defense_rating": defense_rating,
            "opponent_offense_strength": opp_offense_strength,
            "defense_block_factor": defense_block_factor,

            # Combined multipliers for easy application
            "total_shot_multiplier": fatigue_shot_factor * goalie_shot_factor * pace_factor * chemistry_factor,
            "total_block_multiplier": fatigue_block_factor * defense_block_factor
        }

        self.game_context_cache[cache_key] = context
        return context

    def calculate_probability_over_threshold(self, expected: float, std: float,
                                              threshold: float,
                                              dampen_high_prob: bool = True) -> float:
        """
        Calculate probability of exceeding a threshold using Negative Binomial distribution.

        Uses Negative Binomial instead of Normal because:
        1. SOG is a discrete count variable (non-negative integers)
        2. Variance often exceeds mean (overdispersion)
        3. Provides better calibration at probability extremes

        Args:
            expected: Expected value (lambda/mean)
            std: Standard deviation (used to estimate overdispersion)
            threshold: The threshold to exceed (e.g., 2 for "2+ SOG")
            dampen_high_prob: If True, applies dampening to probabilities > 80%
        """
        if expected <= 0:
            return 0.0

        # Use Negative Binomial for overdispersed count data
        # Variance = mean + mean^2/r, where r is the dispersion parameter
        # Solve for r: r = mean^2 / (variance - mean)
        variance = std * std if std > 0 else expected

        # If variance <= mean, fall back to Poisson (no overdispersion)
        if variance <= expected:
            # Poisson distribution: P(X >= k) = 1 - P(X <= k-1)
            # Use Poisson PMF summation
            prob_under = 0.0
            factorial = 1.0
            exp_neg_lambda = math.exp(-expected)
            for k in range(threshold):
                if k > 0:
                    factorial *= k
                prob_under += (expected ** k) * exp_neg_lambda / factorial
            prob_over = 1 - prob_under
        else:
            # Negative Binomial with overdispersion
            # r = mean^2 / (variance - mean)
            r = (expected * expected) / (variance - expected)
            r = max(0.5, min(100, r))  # Clamp r to reasonable range

            # p = r / (r + mean) for NegBin parameterization
            p = r / (r + expected)

            # P(X >= threshold) = 1 - P(X <= threshold - 1)
            # Use recursive formula for NegBin CDF
            prob_under = 0.0
            for k in range(threshold):
                # NegBin PMF: C(k+r-1, k) * p^r * (1-p)^k
                # Use log-space for numerical stability
                log_prob = self._log_negbin_pmf(k, r, p)
                prob_under += math.exp(log_prob)

            prob_over = 1 - prob_under

        prob_over = max(0.0, min(1.0, prob_over))

        # Apply dampening to high probabilities to reduce overconfidence
        # Based on calibration analysis: probabilities > 80% are ~10-20% overconfident
        if dampen_high_prob and prob_over > 0.80:
            # Dampen by 10% of the excess above 80%
            excess = prob_over - 0.80
            prob_over = 0.80 + (excess * 0.90)

        return prob_over

    def _log_negbin_pmf(self, k: int, r: float, p: float) -> float:
        """
        Calculate log of Negative Binomial PMF for numerical stability.
        PMF = C(k+r-1, k) * p^r * (1-p)^k
        """
        # log(C(k+r-1, k)) = lgamma(k+r) - lgamma(k+1) - lgamma(r)
        log_binom = math.lgamma(k + r) - math.lgamma(k + 1) - math.lgamma(r)
        log_prob = log_binom + r * math.log(p) + k * math.log(1 - p)
        return log_prob

    def predict_player_game(self, player_id: int, opponent: str, game_date: str,
                            is_home: bool) -> Dict[str, Any]:
        """
        Predict player's shots and blocked shots for a specific game.

        NOW INTEGRATES ALL GAME-LEVEL FACTORS:
        - Rest/fatigue (back-to-back, well-rested)
        - Opponent goalie quality
        - Line chemistry
        - Special teams / discipline
        - Pace of play

        Returns predictions with:
        - Expected values
        - Probability of hitting various thresholds
        - Confidence ratings
        """
        # Get player stats
        player_stats = self.get_player_historical_stats(player_id, game_date)
        opp_factors = self.get_opponent_defensive_factor(opponent, game_date)

        # Get position baseline
        position = player_stats["position"]
        player_team = player_stats["team"]
        baseline = self.position_baselines.get(position, {"shots": 1.5, "blocked": 0.7})

        # GET GAME CONTEXT (all the game-level factors)
        game_context = self.get_game_context(player_team, opponent, game_date, is_home)

        # Calculate expected shots
        # UPDATED WEIGHTS: 28% recent form (up from 20%), 52% season average, 20% position baseline
        # Increased recent form weight to better capture hot/cold streaks
        if player_stats["games_analyzed"] >= 5:
            expected_shots = (
                0.28 * player_stats["recent_5_shots"] +
                0.52 * player_stats["avg_shots"] +
                0.20 * baseline["shots"]
            )
            shots_std = player_stats["shots_std"]
        elif player_stats["games_analyzed"] > 0:
            expected_shots = (
                0.40 * player_stats["avg_shots"] +
                0.60 * baseline["shots"]
            )
            shots_std = player_stats["shots_std"] * 1.2  # More uncertainty
        else:
            expected_shots = baseline["shots"]
            shots_std = 1.2  # High uncertainty for unknown players

        # Calculate expected blocked shots
        if player_stats["games_analyzed"] >= 5:
            expected_blocked = (
                0.28 * player_stats["recent_5_blocked"] +
                0.52 * player_stats["avg_blocked"] +
                0.20 * baseline["blocked"]
            )
            blocked_std = player_stats["blocked_std"]
        elif player_stats["games_analyzed"] > 0:
            expected_blocked = (
                0.40 * player_stats["avg_blocked"] +
                0.60 * baseline["blocked"]
            )
            blocked_std = player_stats["blocked_std"] * 1.2
        else:
            expected_blocked = baseline["blocked"]
            blocked_std = 0.9

        # Apply opponent factors (basic)
        expected_shots *= opp_factors["shots_factor"]
        expected_blocked *= opp_factors["blocks_factor"]

        # Home/away adjustment (slight home boost for shots)
        if is_home:
            expected_shots *= 1.03
        else:
            expected_blocked *= 1.02  # Away team often blocking more

        # APPLY GAME CONTEXT FACTORS
        # 1. Fatigue (back-to-back reduces shots)
        expected_shots *= game_context["fatigue_shot_factor"]
        expected_blocked *= game_context["fatigue_block_factor"]

        # 2. Opponent goalie quality (weaker goalie = more shots)
        expected_shots *= game_context["goalie_shot_factor"]

        # 3. Pace of play (high-pace game = more shots)
        expected_shots *= game_context["pace_factor"]

        # 4. Line chemistry (good chemistry = better puck movement = more shots)
        expected_shots *= game_context["chemistry_factor"]

        # 5. Defense/blocking matchup (strong opponent offense = more blocking)
        expected_blocked *= game_context["defense_block_factor"]

        # 6. Special teams - boost for power play specialists
        # Check if player is a PP contributor (high shots + high TOI usually means PP time)
        if player_stats["avg_toi"] > 1000 and player_stats["avg_shots"] > 2.0:
            # This player likely gets PP time, boost based on opponent discipline
            expected_shots *= game_context["pp_opportunity_factor"]

        # 7. DEFENSEMEN ADJUSTMENT: Reduce D-men predictions by 5%
        # Based on calibration analysis showing D-men are consistently over-predicted
        if position == "D":
            expected_shots *= 0.95

        # 8. SUPERSTAR REGRESSION: Shrink high expected SOG toward mean
        # Players with exp > 3.0 SOG regress by ~0.65 on average
        if expected_shots > 3.0:
            # Apply 10% regression toward 3.0 for superstars
            regression_amount = (expected_shots - 3.0) * 0.10
            expected_shots -= regression_amount

        # 9. BREAKOUT DETECTION: Boost players on hot streaks
        # If player is shooting 1+ above season average in last 5 games
        if player_stats.get("is_breakout", False):
            breakout_factor = player_stats.get("breakout_factor", 1.0)
            # Apply partial breakout boost (not full, to avoid over-correction)
            expected_shots *= (1.0 + (breakout_factor - 1.0) * 0.5)

        # 10. TOI VOLATILITY: Increase uncertainty for players with inconsistent ice time
        # High TOI coefficient of variation = more uncertainty in prediction
        toi_cv = player_stats.get("toi_cv", 0)
        if toi_cv > 0.15:  # More than 15% TOI variance
            # Increase std by up to 20% for highly volatile TOI
            toi_uncertainty_factor = 1.0 + min(0.20, toi_cv)
            shots_std *= toi_uncertainty_factor

        # Calculate probabilities for various thresholds
        shot_thresholds = [1, 2, 3, 4, 5]
        block_thresholds = [1, 2, 3]

        shot_probabilities = {}
        for thresh in shot_thresholds:
            prob = self.calculate_probability_over_threshold(expected_shots, shots_std, thresh)
            shot_probabilities[f"{thresh}+"] = round(prob, 3)

        block_probabilities = {}
        for thresh in block_thresholds:
            prob = self.calculate_probability_over_threshold(expected_blocked, blocked_std, thresh)
            block_probabilities[f"{thresh}+"] = round(prob, 3)

        # Calculate confidence based on data quality
        # Higher confidence with more games and consistent performance
        # Also factor in TOI volatility (more volatile = lower confidence)
        games_factor = min(1.0, player_stats["games_analyzed"] / 10)
        consistency_factor = max(0.3, 1 - (shots_std / max(expected_shots, 1)))
        toi_stability_factor = max(0.7, 1 - toi_cv)  # Reduce confidence for volatile TOI

        shots_confidence = round((games_factor * 0.5 + consistency_factor * 0.3 + toi_stability_factor * 0.2), 3)
        blocks_confidence = round(games_factor * 0.5 + consistency_factor * 0.3 + 0.2, 3)

        # Calculate variance indicator - how much prediction differs from historical average
        historical_avg = player_stats["avg_shots"]
        deviation_from_avg = expected_shots - historical_avg
        deviation_pct = (deviation_from_avg / historical_avg * 100) if historical_avg > 0 else 0

        # Coefficient of variation (CV) - measure of consistency
        cv = (shots_std / expected_shots * 100) if expected_shots > 0 else 100

        # Determine variance indicator based on CV and deviation
        if cv > 60:
            variance_indicator = "Volatile"  # Very inconsistent player
        elif cv > 40:
            if deviation_pct > 15:
                variance_indicator = "Higher*"  # Above avg but inconsistent
            elif deviation_pct < -15:
                variance_indicator = "Lower*"  # Below avg but inconsistent
            else:
                variance_indicator = "Variable"  # Moderate inconsistency
        else:
            if deviation_pct > 20:
                variance_indicator = "Higher"  # Significantly above historical avg
            elif deviation_pct > 10:
                variance_indicator = "Slight+"  # Slightly above avg
            elif deviation_pct < -20:
                variance_indicator = "Lower"  # Significantly below historical avg
            elif deviation_pct < -10:
                variance_indicator = "Slight-"  # Slightly below avg
            else:
                variance_indicator = "Stable"  # Close to historical avg

        # Check injury status
        injury_status = ""
        injury_weight = 1.0
        is_injured = False
        if self.injury_tracker:
            injury_info = self.injury_tracker.get_player_status(player_stats["player_name"])
            if injury_info:
                injury_status = injury_info.get("status", "")
                injury_weight = injury_info.get("weight", 0.0)
                is_injured = True

        return {
            "player_id": player_id,
            "player_name": player_stats["player_name"],
            "position": position,
            "team": player_team,
            "opponent": opponent,
            "game_date": game_date,
            "is_home": is_home,
            "games_analyzed": player_stats["games_analyzed"],

            # Injury status
            "injury_status": injury_status,
            "injury_weight": injury_weight,
            "is_injured": is_injured,

            # Shots prediction
            "expected_shots": round(expected_shots, 2),
            "shots_std": round(shots_std, 2),
            "shot_probabilities": shot_probabilities,
            "shots_confidence": shots_confidence,
            "historical_avg_shots": round(historical_avg, 2),
            "deviation_from_avg": round(deviation_from_avg, 2),
            "deviation_pct": round(deviation_pct, 1),
            "variance_indicator": variance_indicator,

            # Blocked shots prediction
            "expected_blocked": round(expected_blocked, 2),
            "blocked_std": round(blocked_std, 2),
            "block_probabilities": block_probabilities,
            "blocked_confidence": blocks_confidence,

            # Additional context
            "avg_toi_minutes": round(player_stats["avg_toi"] / 60, 1),
            "recent_5_shots": round(player_stats["recent_5_shots"], 2),
            "recent_5_blocked": round(player_stats["recent_5_blocked"], 2),
            "opponent_shots_factor": round(opp_factors["shots_factor"], 3),
            "opponent_blocks_factor": round(opp_factors["blocks_factor"], 3),

            # Historical max (for context)
            "max_shots_recent": player_stats.get("max_shots", 0),
            "max_blocked_recent": player_stats.get("max_blocked", 0),

            # GAME CONTEXT FACTORS (new)
            "game_context": {
                "is_back_to_back": game_context["is_back_to_back"],
                "is_well_rested": game_context["is_well_rested"],
                "rest_days": game_context["rest_days"],
                "fatigue_factor": round(game_context["fatigue_shot_factor"], 3),
                "opponent_goalie_sv_pct": round(game_context["opponent_goalie_sv_pct"], 3),
                "goalie_factor": round(game_context["goalie_shot_factor"], 3),
                "pace_factor": round(game_context["pace_factor"], 3),
                "chemistry_factor": round(game_context["chemistry_factor"], 3),
                "pp_opportunity_factor": round(game_context["pp_opportunity_factor"], 3),
                "total_shot_multiplier": round(game_context["total_shot_multiplier"], 3),
                "total_block_multiplier": round(game_context["total_block_multiplier"], 3)
            },

            # NEW: Model adjustment factors applied
            "model_adjustments": {
                "is_breakout": player_stats.get("is_breakout", False),
                "breakout_factor": round(player_stats.get("breakout_factor", 1.0), 3),
                "toi_volatility": round(toi_cv, 3),
                "superstar_regression_applied": expected_shots > 3.0,
                "defenseman_reduction_applied": position == "D",
            },

            # Historical accuracy (will be populated by predict_team_players)
            "historical_accuracy_pct": None,
            "historical_predictions": 0,
        }

    def predict_team_players(self, team_abbrev: str, opponent: str, game_date: str,
                             is_home: bool, limit: int = 20,
                             include_injured_out: bool = False,
                             include_historical_accuracy: bool = True) -> List[Dict[str, Any]]:
        """
        Predict stats for all players on a team for a specific game.

        Args:
            include_injured_out: If True, include players who are OUT/IR/LTIR
                                (they'll be flagged but included in results)
            include_historical_accuracy: If True, include 10-day historical accuracy for each player
        """
        # Get historical accuracy data if requested
        player_accuracy = {}
        if include_historical_accuracy:
            try:
                player_accuracy = self.get_player_historical_accuracy(lookback_days=10)
            except Exception:
                pass  # Silently fail if accuracy data unavailable

        with self.get_connection() as conn:
            # Get active players on roster
            players = conn.execute("""
                SELECT DISTINCT pgs.player_id
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE pgs.team_abbrev = ?
                  AND g.game_date < ?
                  AND g.game_date >= date(?, '-30 days')
                GROUP BY pgs.player_id
                HAVING COUNT(*) >= 3
                ORDER BY AVG(pgs.toi_seconds) DESC
                LIMIT ?
            """, (team_abbrev, game_date, game_date, limit)).fetchall()

        predictions = []
        for p in players:
            try:
                pred = self.predict_player_game(p["player_id"], opponent, game_date, is_home)

                # Filter by position-specific TOI thresholds
                position = pred.get("position", "")
                avg_toi_minutes = pred.get("avg_toi_minutes", 0)

                # Forwards (C, L, R) must have 14+ minutes TOI
                if position in ("C", "L", "R") and avg_toi_minutes < 14:
                    continue
                # Defensemen must have 16+ minutes TOI
                if position == "D" and avg_toi_minutes < 16:
                    continue

                # Check injury status - skip players who are OUT unless requested
                injury_weight = pred.get("injury_weight", 1.0)
                injury_status = pred.get("injury_status", "")

                if injury_weight == 0.0 and not include_injured_out:
                    # Player is OUT/IR/LTIR - skip them by default
                    continue

                # For DTD/Questionable players, reduce confidence but keep them
                if injury_status in ("DTD", "QUESTIONABLE"):
                    # Reduce shots confidence to reflect uncertainty
                    pred["shots_confidence"] = round(pred["shots_confidence"] * 0.7, 3)
                    pred["blocked_confidence"] = round(pred["blocked_confidence"] * 0.7, 3)

                # Add historical accuracy if available
                player_id = pred["player_id"]
                if player_id in player_accuracy:
                    acc_data = player_accuracy[player_id]
                    pred["historical_accuracy_pct"] = acc_data["accuracy_pct"]
                    pred["historical_predictions"] = acc_data["predictions"]

                predictions.append(pred)
            except Exception as e:
                continue

        # Sort by expected shots (descending)
        predictions.sort(key=lambda x: x["expected_shots"], reverse=True)
        return predictions

    def predict_game_all_players(self, home_team: str, away_team: str,
                                  game_date: str) -> Dict[str, Any]:
        """
        Predict player stats for all players in a game.
        Automatically filters out injured players (OUT/IR/LTIR) and flags DTD players.
        """
        home_predictions = self.predict_team_players(home_team, away_team, game_date, is_home=True)
        away_predictions = self.predict_team_players(away_team, home_team, game_date, is_home=False)

        # Count injured players
        home_injured = sum(1 for p in home_predictions if p.get("is_injured", False))
        away_injured = sum(1 for p in away_predictions if p.get("is_injured", False))

        # Get list of OUT players from injury tracker (if available)
        home_out = []
        away_out = []
        if self.injury_tracker:
            home_injuries = self.injury_tracker.get_team_injuries(home_team)
            away_injuries = self.injury_tracker.get_team_injuries(away_team)
            home_out = [i["player_name"] for i in home_injuries if i.get("status") in ("OUT", "IR", "LTIR", "SUSPENDED")]
            away_out = [i["player_name"] for i in away_injuries if i.get("status") in ("OUT", "IR", "LTIR", "SUSPENDED")]

        return {
            "game_date": game_date,
            "home_team": home_team,
            "away_team": away_team,
            "home_players": home_predictions,
            "away_players": away_predictions,
            "home_player_count": len(home_predictions),
            "away_player_count": len(away_predictions),
            "injury_summary": {
                "home_dtd_count": home_injured,
                "away_dtd_count": away_injured,
                "home_out_players": home_out,
                "away_out_players": away_out,
                "injuries_loaded": self.injury_tracker is not None
            }
        }

    def get_player_historical_accuracy(self, lookback_days: int = 10,
                                         predictions_dir: str = "predictions") -> Dict[int, Dict]:
        """
        Calculate historical prediction accuracy for all players over the last N days.
        Returns a dictionary mapping player_id to their accuracy stats.

        This is cached and reused for all predictions in a session.
        """
        # Check if we have a cached result
        cache_key = f"accuracy_{lookback_days}"
        if hasattr(self, '_accuracy_cache') and cache_key in self._accuracy_cache:
            return self._accuracy_cache[cache_key]

        import json
        from datetime import datetime, timedelta
        from pathlib import Path
        from collections import defaultdict

        if not hasattr(self, '_accuracy_cache'):
            self._accuracy_cache = {}

        predictions_path = Path(predictions_dir)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        player_stats = defaultdict(lambda: {
            "predictions": 0,
            "correct_2plus": 0,
            "total_expected": 0,
            "total_actual": 0
        })

        with self.get_connection() as conn:
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                pred_file = predictions_path / f"predictions_{date_str}.json"

                if not pred_file.exists():
                    current_date += timedelta(days=1)
                    continue

                try:
                    with open(pred_file, 'r', encoding='utf-8') as f:
                        pred_data = json.load(f)
                except:
                    current_date += timedelta(days=1)
                    continue

                # Get actual results
                actuals = conn.execute("""
                    SELECT pgs.player_id, pgs.shots
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    WHERE g.game_date = ?
                """, (date_str,)).fetchall()

                if not actuals:
                    current_date += timedelta(days=1)
                    continue

                actual_map = {row[0]: row[1] for row in actuals}

                for pred in pred_data.get("all_predictions", []):
                    player_id = pred["player_id"]
                    if player_id not in actual_map:
                        continue

                    actual_sog = actual_map[player_id]
                    expected = pred["expected_shots"]
                    prob_2plus = pred.get("prob_2plus", 0)

                    hit_2plus = actual_sog >= 2
                    predicted_2plus = prob_2plus >= 0.5
                    correct = (predicted_2plus and hit_2plus) or (not predicted_2plus and not hit_2plus)

                    ps = player_stats[player_id]
                    ps["predictions"] += 1
                    ps["correct_2plus"] += 1 if correct else 0
                    ps["total_expected"] += expected
                    ps["total_actual"] += actual_sog

                current_date += timedelta(days=1)

        # Calculate accuracy percentages
        result = {}
        for player_id, stats in player_stats.items():
            if stats["predictions"] >= 1:
                accuracy = (stats["correct_2plus"] / stats["predictions"]) * 100
                result[player_id] = {
                    "accuracy_pct": round(accuracy, 1),
                    "predictions": stats["predictions"],
                    "avg_expected": round(stats["total_expected"] / stats["predictions"], 2),
                    "avg_actual": round(stats["total_actual"] / stats["predictions"], 2)
                }

        self._accuracy_cache[cache_key] = result
        return result

    def compare_predictions_vs_results(self, lookback_days: int = 10,
                                        predictions_dir: str = "predictions") -> Dict[str, Any]:
        """
        Compare predictions from the last N days against actual game results.

        Returns detailed accuracy analysis including:
        - Overall accuracy metrics
        - Per-player accuracy
        - Top 15 most accurate players sorted by team
        - Daily breakdown

        Args:
            lookback_days: Number of days to analyze
            predictions_dir: Directory containing prediction JSON files

        Returns:
            Dictionary with comparison results and top accurate players
        """
        import json
        import os
        from datetime import datetime, timedelta
        from collections import defaultdict
        from pathlib import Path

        predictions_path = Path(predictions_dir)

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        # Collect all comparisons
        all_comparisons = []
        player_stats = defaultdict(lambda: {
            "player_name": "",
            "team": "",
            "position": "",
            "predictions": 0,
            "hits_2plus": 0,
            "hits_3plus": 0,
            "total_expected": 0,
            "total_actual": 0,
            "total_error": 0,
            "games": []
        })
        daily_stats = {}

        with self.get_connection() as conn:
            # Iterate through each day
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                pred_file = predictions_path / f"predictions_{date_str}.json"

                if not pred_file.exists():
                    current_date += timedelta(days=1)
                    continue

                # Load predictions
                with open(pred_file, 'r', encoding='utf-8') as f:
                    pred_data = json.load(f)

                # Get actual results for this date
                actuals = conn.execute("""
                    SELECT pgs.player_id, p.full_name, pgs.shots, pgs.toi_seconds,
                           pgs.team_abbrev, g.game_id
                    FROM player_game_stats pgs
                    JOIN games g ON pgs.game_id = g.game_id
                    JOIN players p ON pgs.player_id = p.player_id
                    WHERE g.game_date = ?
                """, (date_str,)).fetchall()

                if not actuals:
                    current_date += timedelta(days=1)
                    continue

                actual_map = {row[0]: {
                    "name": row[1],
                    "sog": row[2],
                    "toi": row[3],
                    "team": row[4]
                } for row in actuals}

                # Compare predictions to actuals
                day_matched = 0
                day_hits_2plus = 0
                day_total = 0

                for pred in pred_data.get("all_predictions", []):
                    player_id = pred["player_id"]
                    if player_id not in actual_map:
                        continue

                    actual = actual_map[player_id]
                    expected = pred["expected_shots"]
                    actual_sog = actual["sog"]
                    prob_2plus = pred.get("prob_2plus", 0)
                    prob_3plus = pred.get("prob_3plus", 0)

                    hit_2plus = actual_sog >= 2
                    hit_3plus = actual_sog >= 3
                    predicted_2plus = prob_2plus >= 0.5
                    predicted_3plus = prob_3plus >= 0.5

                    # Accuracy: prediction matched outcome
                    correct_2plus = (predicted_2plus and hit_2plus) or (not predicted_2plus and not hit_2plus)

                    comparison = {
                        "date": date_str,
                        "player_id": player_id,
                        "player_name": pred["player_name"],
                        "team": pred.get("team", actual["team"]),
                        "position": pred.get("position", ""),
                        "expected_sog": expected,
                        "actual_sog": actual_sog,
                        "prob_2plus": prob_2plus,
                        "prob_3plus": prob_3plus,
                        "hit_2plus": hit_2plus,
                        "hit_3plus": hit_3plus,
                        "correct_2plus": correct_2plus,
                        "error": actual_sog - expected
                    }
                    all_comparisons.append(comparison)

                    # Update player stats
                    ps = player_stats[player_id]
                    ps["player_name"] = pred["player_name"]
                    ps["team"] = pred.get("team", actual["team"])
                    ps["position"] = pred.get("position", "")
                    ps["predictions"] += 1
                    ps["hits_2plus"] += 1 if (predicted_2plus == hit_2plus) else 0
                    ps["hits_3plus"] += 1 if (predicted_3plus == hit_3plus) else 0
                    ps["total_expected"] += expected
                    ps["total_actual"] += actual_sog
                    ps["total_error"] += abs(actual_sog - expected)
                    ps["games"].append({
                        "date": date_str,
                        "expected": expected,
                        "actual": actual_sog,
                        "correct": correct_2plus
                    })

                    day_matched += 1
                    day_hits_2plus += 1 if correct_2plus else 0
                    day_total += 1

                if day_total > 0:
                    daily_stats[date_str] = {
                        "predictions": day_total,
                        "matched": day_matched,
                        "accuracy_2plus": day_hits_2plus / day_total if day_total > 0 else 0
                    }

                current_date += timedelta(days=1)

        if not all_comparisons:
            return {"error": "No predictions found for the specified date range"}

        # Calculate overall metrics
        total = len(all_comparisons)
        correct_2plus = sum(1 for c in all_comparisons if c["correct_2plus"])
        hits_2plus = sum(1 for c in all_comparisons if c["hit_2plus"])
        mae = sum(abs(c["error"]) for c in all_comparisons) / total

        # Calculate player accuracy rates
        player_accuracy = []
        for player_id, stats in player_stats.items():
            if stats["predictions"] >= 3:  # Minimum 3 predictions for accuracy calc
                accuracy = stats["hits_2plus"] / stats["predictions"]
                player_accuracy.append({
                    "player_id": player_id,
                    "player_name": stats["player_name"],
                    "team": stats["team"],
                    "position": stats["position"],
                    "predictions": stats["predictions"],
                    "accuracy_2plus": round(accuracy * 100, 1),
                    "avg_expected": round(stats["total_expected"] / stats["predictions"], 2),
                    "avg_actual": round(stats["total_actual"] / stats["predictions"], 2),
                    "mae": round(stats["total_error"] / stats["predictions"], 2)
                })

        # Sort by accuracy (descending), then by predictions (descending)
        player_accuracy.sort(key=lambda x: (-x["accuracy_2plus"], -x["predictions"]))

        # Get top 15 most accurate players
        top_15 = player_accuracy[:15]

        # Sort top 15 by team for display
        top_15_by_team = sorted(top_15, key=lambda x: (x["team"], -x["accuracy_2plus"]))

        # Group top 15 by team
        top_15_grouped = defaultdict(list)
        for p in top_15_by_team:
            top_15_grouped[p["team"]].append(p)

        return {
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "days_analyzed": lookback_days,
            "total_predictions": total,
            "overall_accuracy": {
                "correct_2plus_predictions": correct_2plus,
                "accuracy_2plus": round(correct_2plus / total * 100, 1),
                "actual_2plus_rate": round(hits_2plus / total * 100, 1),
                "mae": round(mae, 2)
            },
            "daily_breakdown": daily_stats,
            "top_15_accurate_players": top_15,
            "top_15_by_team": dict(top_15_grouped),
            "all_player_stats": player_accuracy
        }

    def print_accuracy_summary(self, lookback_days: int = 10) -> None:
        """
        Print a formatted accuracy summary comparing predictions vs results.
        """
        results = self.compare_predictions_vs_results(lookback_days)

        if "error" in results:
            print(f"Error: {results['error']}")
            return

        print("=" * 70)
        print("PREDICTION ACCURACY SUMMARY")
        print("=" * 70)
        print(f"Period: {results['period']}")
        print(f"Total Predictions: {results['total_predictions']}")
        print()

        overall = results["overall_accuracy"]
        print("Overall Accuracy:")
        print(f"  2+ SOG Prediction Accuracy: {overall['accuracy_2plus']}%")
        print(f"  Actual 2+ SOG Rate: {overall['actual_2plus_rate']}%")
        print(f"  Mean Absolute Error: {overall['mae']} SOG")
        print()

        print("Daily Breakdown:")
        print(f"  {'Date':<12} | {'Predictions':>11} | {'Accuracy':>8}")
        print("-" * 40)
        for date, stats in sorted(results["daily_breakdown"].items()):
            acc = stats["accuracy_2plus"] * 100
            print(f"  {date:<12} | {stats['predictions']:>11} | {acc:>7.1f}%")
        print()

        print("=" * 70)
        print("TOP 15 MOST ACCURATE PLAYERS (Sorted by Team)")
        print("=" * 70)
        print(f"{'Team':<5} | {'Player':<22} | {'Pos':<3} | {'Games':>5} | {'Acc':>6} | {'Avg Exp':>7} | {'Avg Act':>7}")
        print("-" * 70)

        for team, players in sorted(results["top_15_by_team"].items()):
            for p in players:
                print(f"{p['team']:<5} | {p['player_name'][:22]:<22} | {p['position']:<3} | {p['predictions']:>5} | {p['accuracy_2plus']:>5.1f}% | {p['avg_expected']:>7.2f} | {p['avg_actual']:>7.2f}")

        print("=" * 70)


class PlayerPredictionBacktester:
    """Backtester for player prediction model."""

    def __init__(self, db_path: str = "nhl_stats.db"):
        self.db_path = db_path
        self.predictor = EnhancedPlayerPredictor(db_path)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def backtest_player_predictions(self, test_start: str, test_end: str,
                                    sample_size: int = 500) -> Dict[str, Any]:
        """
        Backtest player predictions on historical data.
        Evaluates shots and blocked shots predictions.
        """
        print(f"Player prediction backtest: {test_start} to {test_end}")

        with self.get_connection() as conn:
            # Get sample of player-games to test
            player_games = conn.execute("""
                SELECT
                    pgs.player_id,
                    pgs.game_id,
                    pgs.team_abbrev,
                    pgs.opponent_abbrev,
                    pgs.is_home,
                    pgs.shots as actual_shots,
                    pgs.blocked_shots as actual_blocked,
                    g.game_date
                FROM player_game_stats pgs
                JOIN games g ON pgs.game_id = g.game_id
                WHERE g.game_date >= ? AND g.game_date <= ?
                  AND pgs.toi_seconds > 600
                ORDER BY RANDOM()
                LIMIT ?
            """, (test_start, test_end, sample_size)).fetchall()

        print(f"Testing on {len(player_games)} player-games...")

        results = {
            "shots": {"predictions": [], "mae": 0, "rmse": 0, "threshold_accuracy": {}},
            "blocked": {"predictions": [], "mae": 0, "rmse": 0, "threshold_accuracy": {}}
        }

        shot_errors = []
        block_errors = []
        shot_threshold_results = {1: {"correct": 0, "total": 0}, 2: {"correct": 0, "total": 0},
                                  3: {"correct": 0, "total": 0}}
        block_threshold_results = {1: {"correct": 0, "total": 0}, 2: {"correct": 0, "total": 0}}

        for pg in player_games:
            try:
                pred = self.predictor.predict_player_game(
                    pg["player_id"],
                    pg["opponent_abbrev"],
                    pg["game_date"],
                    pg["is_home"]
                )

                actual_shots = pg["actual_shots"] or 0
                actual_blocked = pg["actual_blocked"] or 0

                # Calculate errors
                shot_error = pred["expected_shots"] - actual_shots
                block_error = pred["expected_blocked"] - actual_blocked

                shot_errors.append(shot_error)
                block_errors.append(block_error)

                # Check threshold predictions
                for thresh in [1, 2, 3]:
                    prob_key = f"{thresh}+"
                    if prob_key in pred["shot_probabilities"]:
                        predicted_over = pred["shot_probabilities"][prob_key] > 0.5
                        actual_over = actual_shots >= thresh
                        shot_threshold_results[thresh]["total"] += 1
                        if predicted_over == actual_over:
                            shot_threshold_results[thresh]["correct"] += 1

                for thresh in [1, 2]:
                    prob_key = f"{thresh}+"
                    if prob_key in pred["block_probabilities"]:
                        predicted_over = pred["block_probabilities"][prob_key] > 0.5
                        actual_over = actual_blocked >= thresh
                        block_threshold_results[thresh]["total"] += 1
                        if predicted_over == actual_over:
                            block_threshold_results[thresh]["correct"] += 1

            except Exception as e:
                continue

        # Calculate metrics
        if shot_errors:
            results["shots"]["mae"] = round(statistics.mean([abs(e) for e in shot_errors]), 3)
            results["shots"]["rmse"] = round(math.sqrt(statistics.mean([e**2 for e in shot_errors])), 3)
            results["shots"]["bias"] = round(statistics.mean(shot_errors), 3)

        if block_errors:
            results["blocked"]["mae"] = round(statistics.mean([abs(e) for e in block_errors]), 3)
            results["blocked"]["rmse"] = round(math.sqrt(statistics.mean([e**2 for e in block_errors])), 3)
            results["blocked"]["bias"] = round(statistics.mean(block_errors), 3)

        # Threshold accuracy
        for thresh, data in shot_threshold_results.items():
            if data["total"] > 0:
                results["shots"]["threshold_accuracy"][f"{thresh}+"] = {
                    "accuracy": round(data["correct"] / data["total"], 3),
                    "count": data["total"]
                }

        for thresh, data in block_threshold_results.items():
            if data["total"] > 0:
                results["blocked"]["threshold_accuracy"][f"{thresh}+"] = {
                    "accuracy": round(data["correct"] / data["total"], 3),
                    "count": data["total"]
                }

        results["player_games_tested"] = len(shot_errors)
        return results


def print_player_prediction(pred: Dict[str, Any]) -> None:
    """Print formatted player prediction."""
    print(f"\n{'='*60}")
    print(f"PLAYER: {pred['player_name']} ({pred['position']}) - {pred['team']}")
    print(f"vs {pred['opponent']} | {'HOME' if pred['is_home'] else 'AWAY'} | {pred['game_date']}")
    print(f"{'='*60}")

    print(f"\n--- SHOTS ON GOAL ---")
    print(f"  Expected: {pred['expected_shots']:.1f} (+/- {pred['shots_std']:.1f})")
    print(f"  Confidence: {pred['shots_confidence']:.0%}")
    print(f"  Probabilities:")
    for thresh, prob in pred['shot_probabilities'].items():
        bar = '#' * int(prob * 20)
        print(f"    {thresh} shots: {prob:.0%} {bar}")

    print(f"\n--- BLOCKED SHOTS ---")
    print(f"  Expected: {pred['expected_blocked']:.1f} (+/- {pred['blocked_std']:.1f})")
    print(f"  Confidence: {pred['blocked_confidence']:.0%}")
    print(f"  Probabilities:")
    for thresh, prob in pred['block_probabilities'].items():
        bar = '#' * int(prob * 20)
        print(f"    {thresh} blocks: {prob:.0%} {bar}")

    print(f"\n--- CONTEXT ---")
    print(f"  Games Analyzed: {pred['games_analyzed']}")
    print(f"  Avg TOI: {pred['avg_toi_minutes']:.0f} min")
    print(f"  Recent 5 avg: {pred['recent_5_shots']:.1f} SOG, {pred['recent_5_blocked']:.1f} BLK")
    print(f"  Opponent factors: SOG={pred['opponent_shots_factor']:.2f}x, BLK={pred['opponent_blocks_factor']:.2f}x")


def print_player_backtest_results(results: Dict[str, Any]) -> None:
    """Print player prediction backtest results."""
    print("\n" + "=" * 60)
    print("PLAYER PREDICTION BACKTEST RESULTS")
    print("=" * 60)
    print(f"Player-games tested: {results['player_games_tested']}")

    print("\n--- SHOTS ON GOAL ---")
    print(f"  MAE: {results['shots']['mae']:.2f}")
    print(f"  RMSE: {results['shots']['rmse']:.2f}")
    print(f"  Bias: {results['shots']['bias']:+.2f}")
    print("  Threshold Accuracy:")
    for thresh, data in results['shots']['threshold_accuracy'].items():
        print(f"    {thresh}: {data['accuracy']:.1%} ({data['count']} predictions)")

    print("\n--- BLOCKED SHOTS ---")
    print(f"  MAE: {results['blocked']['mae']:.2f}")
    print(f"  RMSE: {results['blocked']['rmse']:.2f}")
    print(f"  Bias: {results['blocked']['bias']:+.2f}")
    print("  Threshold Accuracy:")
    for thresh, data in results['blocked']['threshold_accuracy'].items():
        print(f"    {thresh}: {data['accuracy']:.1%} ({data['count']} predictions)")

    print("\n" + "=" * 60)


def run_player_prediction_demo():
    """Demo the player prediction system."""
    print("NHL Player Prediction Demo")
    print("=" * 60)

    predictor = EnhancedPlayerPredictor(use_injuries=True)

    # Get a sample game
    conn = sqlite3.connect("nhl_stats.db")
    conn.row_factory = sqlite3.Row
    game = conn.execute("""
        SELECT home_team_abbrev, away_team_abbrev, game_date
        FROM games
        WHERE home_score IS NOT NULL
        ORDER BY game_date DESC
        LIMIT 1
    """).fetchone()
    conn.close()

    if game:
        print(f"\nPredicting for: {game['away_team_abbrev']} @ {game['home_team_abbrev']} ({game['game_date']})")

        # Get predictions for home team
        print(f"\n{game['home_team_abbrev']} Players:")
        home_preds = predictor.predict_team_players(
            game['home_team_abbrev'],
            game['away_team_abbrev'],
            game['game_date'],
            is_home=True,
            limit=5
        )

        for pred in home_preds[:3]:
            print_player_prediction(pred)


if __name__ == "__main__":
    run_enhanced_backtest()
