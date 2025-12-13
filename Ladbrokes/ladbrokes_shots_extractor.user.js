// ==UserScript==
// @name         Ladbrokes NHL Player Shots Extractor
// @namespace    http://tampermonkey.net/
// @version      2.1
// @description  Extract NHL Player Shots on Goal markets from Ladbrokes with date/team selection
// @match        https://www.ladbrokes.com.au/sports/ice-hockey/*
// @match        https://www.ladbrokes.com.au/sports/*/ice-hockey/*
// @grant        GM_setClipboard
// @grant        GM_notification
// ==/UserScript==

(function() {
    'use strict';

    // NHL team abbreviations (matching EnhancedPlayerPredictor)
    const NHL_TEAMS = [
        'ANA', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL', 'DET',
        'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR', 'OTT',
        'PHI', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'UTA', 'VAN', 'VGK',
        'WPG', 'WSH'
    ];

    // Team name mappings (URL slug -> standard abbreviation)
    // Maps both city names and team names from Ladbrokes URLs
    const TEAM_URL_MAPPINGS = {
        // City/region names (primary - these appear in URLs)
        'anaheim': 'ANA',
        'boston': 'BOS',
        'buffalo': 'BUF',
        'carolina': 'CAR',
        'columbus': 'CBJ',
        'calgary': 'CGY',
        'chicago': 'CHI',
        'colorado': 'COL',
        'dallas': 'DAL',
        'detroit': 'DET',
        'edmonton': 'EDM',
        'florida': 'FLA',
        'los-angeles': 'LAK',
        'minnesota': 'MIN',
        'montreal': 'MTL',
        'new-jersey': 'NJD',
        'nashville': 'NSH',
        'new-york-islanders': 'NYI',
        'ny-islanders': 'NYI',
        'new-york-rangers': 'NYR',
        'ny-rangers': 'NYR',
        'ottawa': 'OTT',
        'philadelphia': 'PHI',
        'pittsburgh': 'PIT',
        'seattle': 'SEA',
        'san-jose': 'SJS',
        'st-louis': 'STL',
        'tampa-bay': 'TBL',
        'toronto': 'TOR',
        'utah': 'UTA',
        'vancouver': 'VAN',
        'vegas': 'VGK',
        'las-vegas': 'VGK',
        'winnipeg': 'WPG',
        'washington': 'WSH',
        // Team nicknames (backup)
        'ducks': 'ANA',
        'bruins': 'BOS',
        'sabres': 'BUF',
        'hurricanes': 'CAR',
        'blue-jackets': 'CBJ',
        'flames': 'CGY',
        'blackhawks': 'CHI',
        'avalanche': 'COL',
        'stars': 'DAL',
        'red-wings': 'DET',
        'oilers': 'EDM',
        'panthers': 'FLA',
        'kings': 'LAK',
        'wild': 'MIN',
        'canadiens': 'MTL',
        'devils': 'NJD',
        'predators': 'NSH',
        'islanders': 'NYI',
        'rangers': 'NYR',
        'senators': 'OTT',
        'flyers': 'PHI',
        'penguins': 'PIT',
        'kraken': 'SEA',
        'sharks': 'SJS',
        'blues': 'STL',
        'lightning': 'TBL',
        'maple-leafs': 'TOR',
        'leafs': 'TOR',
        'hockey-club': 'UTA',
        'canucks': 'VAN',
        'golden-knights': 'VGK',
        'jets': 'WPG',
        'capitals': 'WSH'
    };

    function detectTeamsFromURL() {
        // Parse teams from URL like:
        // /sports/ice-hockey/usa/nhl/dallas-stars-vs-florida-panthers/8bc9cb1e-...
        const url = window.location.href.toLowerCase();

        console.log('[Extractor] Parsing URL:', url);

        // Try to find the matchup segment in URL
        // Common patterns: "team1-vs-team2", "team1-at-team2", "team1-@-team2"
        const urlParts = url.split('/');
        let matchupSegment = null;

        for (const part of urlParts) {
            if (part.includes('-vs-') || part.includes('-at-') || part.includes('-@-')) {
                matchupSegment = part;
                break;
            }
        }

        if (!matchupSegment) {
            console.log('[Extractor] No matchup found in URL');
            return { away: null, home: null };
        }

        console.log('[Extractor] Matchup segment:', matchupSegment);

        // Split by vs/at to get team parts
        let teamParts;
        if (matchupSegment.includes('-vs-')) {
            teamParts = matchupSegment.split('-vs-');
        } else if (matchupSegment.includes('-at-')) {
            teamParts = matchupSegment.split('-at-');
        } else {
            teamParts = matchupSegment.split('-@-');
        }

        if (teamParts.length !== 2) {
            console.log('[Extractor] Could not split teams');
            return { away: null, home: null };
        }

        console.log('[Extractor] Team parts:', teamParts[0], 'vs', teamParts[1]);

        // Function to find team abbreviation from URL segment
        // Handles formats like "dallas-stars", "florida-panthers", "new-york-rangers"
        function findTeamAbbrev(segment) {
            // Clean up the segment (remove any trailing IDs or numbers)
            segment = segment.replace(/[^a-z-]/g, '').replace(/-+$/, '');

            console.log('[Extractor] Finding team for segment:', segment);

            // Try exact match on full segment first (e.g., "dallas-stars")
            if (TEAM_URL_MAPPINGS[segment]) {
                return TEAM_URL_MAPPINGS[segment];
            }

            // Split segment into words and try matching
            const words = segment.split('-');

            // Try city name (first word or first two words)
            if (TEAM_URL_MAPPINGS[words[0]]) {
                return TEAM_URL_MAPPINGS[words[0]];
            }

            // Try two-word city (e.g., "new-york", "los-angeles", "tampa-bay", "san-jose", "st-louis")
            if (words.length >= 2) {
                const twoWordCity = words[0] + '-' + words[1];
                if (TEAM_URL_MAPPINGS[twoWordCity]) {
                    return TEAM_URL_MAPPINGS[twoWordCity];
                }
            }

            // Try three-word team (e.g., "new-york-rangers", "new-york-islanders")
            if (words.length >= 3) {
                const threeWordTeam = words[0] + '-' + words[1] + '-' + words[2];
                if (TEAM_URL_MAPPINGS[threeWordTeam]) {
                    return TEAM_URL_MAPPINGS[threeWordTeam];
                }
            }

            // Try team nickname (last word, e.g., "stars", "panthers")
            const nickname = words[words.length - 1];
            if (TEAM_URL_MAPPINGS[nickname]) {
                return TEAM_URL_MAPPINGS[nickname];
            }

            // Try two-word nickname (e.g., "golden-knights", "blue-jackets", "red-wings", "maple-leafs")
            if (words.length >= 2) {
                const twoWordNickname = words[words.length - 2] + '-' + words[words.length - 1];
                if (TEAM_URL_MAPPINGS[twoWordNickname]) {
                    return TEAM_URL_MAPPINGS[twoWordNickname];
                }
            }

            // Fallback: try matching any known team name within the segment
            for (const [teamSlug, abbrev] of Object.entries(TEAM_URL_MAPPINGS)) {
                if (segment.includes(teamSlug)) {
                    return abbrev;
                }
            }

            console.log('[Extractor] No match found for:', segment);
            return null;
        }

        const awayTeam = findTeamAbbrev(teamParts[0]);
        const homeTeam = findTeamAbbrev(teamParts[1]);

        console.log('[Extractor] Detected teams:', awayTeam, '@', homeTeam);

        return { away: awayTeam, home: homeTeam };
    }

    function extractPlayerShots() {
        const results = [];

        // Find all market titles for Player Shots O/U
        const marketTitles = document.querySelectorAll('[data-testid="market-title"]');

        marketTitles.forEach(titleEl => {
            const titleText = titleEl.textContent.trim();

            // Match "Player Shots O/U - Player Name (Line)"
            const match = titleText.match(/Player Shots O\/U - (.+?) \((\d+\.?\d*)\)/);
            if (!match) return;

            const playerName = match[1].trim();
            const line = parseFloat(match[2]);

            // Find the parent market container
            const marketContainer = titleEl.closest('[class*="market"]') || titleEl.parentElement?.parentElement;
            if (!marketContainer) return;

            // Find all price buttons in this market
            const priceButtons = marketContainer.querySelectorAll('[data-testid="price-button-odds"]');

            // Usually first is Over, second is Under
            const overOdds = priceButtons[0]?.textContent?.trim();
            const underOdds = priceButtons[1]?.textContent?.trim();

            results.push({
                player: playerName,
                line: line,
                over_odds: overOdds ? parseFloat(overOdds) : null,
                under_odds: underOdds ? parseFloat(underOdds) : null
            });
        });

        return results;
    }

    function createUI() {
        // Remove existing UI if any
        const existing = document.getElementById('lb-extract-container');
        if (existing) existing.remove();

        // Create container
        const container = document.createElement('div');
        container.id = 'lb-extract-container';
        container.style.cssText = `
            position: fixed;
            top: 10px;
            right: 10px;
            z-index: 999999;
            background: #1a1a2e;
            padding: 15px;
            border-radius: 10px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.5);
            font-family: Arial, sans-serif;
            color: white;
            min-width: 280px;
        `;

        // Get today's date in YYYY-MM-DD format
        const today = new Date();
        const defaultDate = today.toISOString().split('T')[0];

        // Detect teams from URL
        const detectedTeams = detectTeamsFromURL();

        // Show detection status
        const detectionStatus = (detectedTeams.away && detectedTeams.home)
            ? `<span style="color: #4CAF50;">✓ ${detectedTeams.away} @ ${detectedTeams.home}</span>`
            : `<span style="color: #ff9800;">⚠ Select teams manually</span>`;

        container.innerHTML = `
            <div style="font-weight: bold; font-size: 16px; margin-bottom: 8px; color: #e53935;">
                🏒 NHL Shots Extractor
            </div>
            <div style="font-size: 11px; margin-bottom: 12px; color: #888;">
                Auto-detected: ${detectionStatus}
            </div>

            <div style="margin-bottom: 10px;">
                <label style="display: block; font-size: 12px; margin-bottom: 4px; color: #aaa;">Game Date:</label>
                <input type="date" id="lb-game-date" value="${defaultDate}" style="
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #444;
                    border-radius: 5px;
                    background: #2a2a3e;
                    color: white;
                    font-size: 14px;
                    box-sizing: border-box;
                ">
            </div>

            <div style="margin-bottom: 10px;">
                <label style="display: block; font-size: 12px; margin-bottom: 4px; color: #aaa;">Away Team:</label>
                <select id="lb-away-team" style="
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #444;
                    border-radius: 5px;
                    background: #2a2a3e;
                    color: white;
                    font-size: 14px;
                    box-sizing: border-box;
                ">
                    ${NHL_TEAMS.map(t => `<option value="${t}" ${t === detectedTeams.away ? 'selected' : ''}>${t}</option>`).join('')}
                </select>
            </div>

            <div style="margin-bottom: 12px;">
                <label style="display: block; font-size: 12px; margin-bottom: 4px; color: #aaa;">Home Team:</label>
                <select id="lb-home-team" style="
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #444;
                    border-radius: 5px;
                    background: #2a2a3e;
                    color: white;
                    font-size: 14px;
                    box-sizing: border-box;
                ">
                    ${NHL_TEAMS.map(t => `<option value="${t}" ${t === detectedTeams.home ? 'selected' : ''}>${t}</option>`).join('')}
                </select>
            </div>

            <button id="lb-extract-btn" style="
                width: 100%;
                padding: 12px;
                background: linear-gradient(135deg, #e53935, #c62828);
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                font-weight: bold;
                font-size: 14px;
                transition: transform 0.2s;
            ">
                📊 Extract Shots
            </button>

            <div id="lb-status" style="
                margin-top: 10px;
                font-size: 12px;
                color: #aaa;
                text-align: center;
            "></div>

            <button id="lb-minimize" style="
                position: absolute;
                top: 5px;
                right: 5px;
                background: none;
                border: none;
                color: #666;
                cursor: pointer;
                font-size: 16px;
            ">✕</button>
        `;

        document.body.appendChild(container);

        // Add event listeners
        document.getElementById('lb-extract-btn').onclick = handleExtract;
        document.getElementById('lb-extract-btn').onmouseover = (e) => e.target.style.transform = 'scale(1.02)';
        document.getElementById('lb-extract-btn').onmouseout = (e) => e.target.style.transform = 'scale(1)';
        document.getElementById('lb-minimize').onclick = () => {
            container.style.display = 'none';
            addMinimizedButton();
        };
    }

    function addMinimizedButton() {
        const btn = document.createElement('button');
        btn.id = 'lb-restore-btn';
        btn.textContent = '🏒';
        btn.style.cssText = `
            position: fixed;
            top: 10px;
            right: 10px;
            z-index: 999999;
            padding: 10px 15px;
            background: #e53935;
            color: white;
            border: none;
            border-radius: 50%;
            cursor: pointer;
            font-size: 20px;
        `;
        btn.onclick = () => {
            btn.remove();
            document.getElementById('lb-extract-container').style.display = 'block';
        };
        document.body.appendChild(btn);
    }

    function handleExtract() {
        const gameDate = document.getElementById('lb-game-date').value;
        const awayTeam = document.getElementById('lb-away-team').value;
        const homeTeam = document.getElementById('lb-home-team').value;
        const statusEl = document.getElementById('lb-status');

        if (!gameDate) {
            statusEl.textContent = '❌ Please select a date';
            statusEl.style.color = '#e53935';
            return;
        }

        if (awayTeam === homeTeam) {
            statusEl.textContent = '❌ Teams must be different';
            statusEl.style.color = '#e53935';
            return;
        }

        const data = extractPlayerShots();

        if (data.length === 0) {
            statusEl.textContent = '❌ No Player Shots markets found. Expand the accordion first.';
            statusEl.style.color = '#e53935';
            return;
        }

        // Create output object
        const output = {
            source: 'ladbrokes',
            game_date: gameDate,
            away_team: awayTeam,
            home_team: homeTeam,
            matchup: `${awayTeam} @ ${homeTeam}`,
            extracted_at: new Date().toISOString(),
            game_url: window.location.href,
            player_count: data.length,
            players: data
        };

        // Generate filename
        const filename = `ladbrokes_${gameDate}_${awayTeam}_${homeTeam}.json`;

        // Copy to clipboard with instructions
        const clipboardContent = JSON.stringify(output, null, 2);
        GM_setClipboard(clipboardContent);

        // Log to console
        console.log('='.repeat(60));
        console.log('LADBROKES PLAYER SHOTS EXTRACTED');
        console.log('='.repeat(60));
        console.log(`Date: ${gameDate}`);
        console.log(`Matchup: ${awayTeam} @ ${homeTeam}`);
        console.log(`Players: ${data.length}`);
        console.log('-'.repeat(60));
        data.forEach(p => {
            console.log(`${p.player} (${p.line}): Over ${p.over_odds} | Under ${p.under_odds}`);
        });
        console.log('='.repeat(60));
        console.log(`Filename: ${filename}`);
        console.log('JSON copied to clipboard!');
        console.log('='.repeat(60));

        // Update status
        statusEl.innerHTML = `
            ✅ Extracted ${data.length} players!<br>
            <span style="color: #4CAF50;">JSON copied to clipboard</span><br>
            <span style="font-size: 10px; color: #888;">Save as: ${filename}</span>
        `;
        statusEl.style.color = '#4CAF50';

        // Show notification
        if (typeof GM_notification !== 'undefined') {
            GM_notification({
                title: 'Shots Extracted!',
                text: `${data.length} players - ${awayTeam} @ ${homeTeam}`,
                timeout: 3000
            });
        }
    }

    // Initialize
    function init() {
        setTimeout(createUI, 2000);

        // Re-create UI if page changes (SPA navigation)
        const observer = new MutationObserver(() => {
            if (!document.getElementById('lb-extract-container') && !document.getElementById('lb-restore-btn')) {
                setTimeout(createUI, 1000);
            }
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
