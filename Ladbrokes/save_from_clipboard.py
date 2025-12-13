"""Save Ladbrokes JSON from clipboard to file."""

import json
import sys
from pathlib import Path

def save_from_clipboard():
    """Read JSON from clipboard and save to Ladbrokes folder."""
    try:
        import pyperclip
        clipboard_content = pyperclip.paste()
    except ImportError:
        print("pyperclip not installed. Install with: pip install pyperclip")
        print("\nAlternatively, paste the JSON content below and press Ctrl+Z then Enter:")
        lines = []
        try:
            while True:
                lines.append(input())
        except EOFError:
            pass
        clipboard_content = '\n'.join(lines)

    try:
        data = json.loads(clipboard_content)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in clipboard - {e}")
        return None

    # Validate required fields
    required = ['game_date', 'away_team', 'home_team', 'players']
    for field in required:
        if field not in data:
            print(f"Error: Missing required field '{field}'")
            return None

    # Generate filename
    game_date = data['game_date']
    away_team = data['away_team']
    home_team = data['home_team']
    filename = f"ladbrokes_{game_date}_{away_team}_{home_team}.json"

    # Save to Ladbrokes folder
    output_path = Path(__file__).parent / filename
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    print(f"Saved: {output_path}")
    print(f"  Date: {game_date}")
    print(f"  Matchup: {away_team} @ {home_team}")
    print(f"  Players: {len(data['players'])}")

    return output_path


if __name__ == "__main__":
    save_from_clipboard()
