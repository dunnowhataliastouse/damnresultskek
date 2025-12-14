# NHL Shots on Goal Predictions
## --threshold

*Generated: 2025-12-14 19:49:13*

*Injury data loaded: 129 players on injury report*

---

---

## Top 15 Shooters (Sorted by Team, then Confidence)

| Rank | Player | Team | Matchup | Exp SOG | Hist Avg | Variance | P(2+) | P(3+) | Status |
|------|--------|------|---------|---------|----------|----------|-------|-------|--------|

---

## High Confidence Picks (P(2+) > 70%) - Sorted by Team, then Confidence

*No players with >70% probability for 2+ shots.*

---

## Legend

### Columns
- **Exp SOG**: Expected Shots on Goal (prediction)
- **Hist Avg**: Historical average SOG over recent games
- **Variance**: How prediction compares to historical average
- **P(2+)**: Probability of 2 or more shots
- **P(3+)**: Probability of 3 or more shots
- **Status**: Injury status (blank = healthy)

### Variance Indicators
| Indicator | Meaning |
|-----------|---------|
| **Stable** | Prediction close to historical average (consistent player) |
| **Slight+** | Prediction 10-20% above historical average |
| **Slight-** | Prediction 10-20% below historical average |
| **Higher** | Prediction >20% above historical average |
| **Lower** | Prediction >20% below historical average |
| **Variable** | Player has moderate game-to-game variance |
| **Higher*** | Above average but inconsistent (use caution) |
| **Lower*** | Below average but inconsistent (use caution) |
| **Volatile** | High variance player - unpredictable |

### Injury Status
| Status | Meaning |
|--------|---------|
| **OUT** | Confirmed out, not playing |
| **IR** | Injured Reserve |
| **LTIR** | Long-Term Injured Reserve |
| **DTD** | Day-to-Day (may or may not play) |
| **SUSPENDED** | Suspended from play |

---

*Predictions generated using EnhancedPlayerPredictor with position-specific TOI filtering*
- Forwards (C, L, R): 14+ min average TOI
- Defensemen (D): 16+ min average TOI
- Injury data from Daily Faceoff (when available)