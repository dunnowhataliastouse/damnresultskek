"""
GitHub Auto-Commit for NHL Predictions
Automatically commits prediction results to a GitHub repository.
"""

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List


class GitHubCommitter:
    """
    Handles automatic git commits and pushes for prediction results.

    Usage:
        committer = GitHubCommitter("https://github.com/user/repo")
        committer.commit_results(game_date, predictions_data)
    """

    REPO_URL = "https://github.com/dunnowhataliastouse/damnresultskek"

    def __init__(self, repo_url: str = None, local_path: str = "."):
        """
        Initialize the GitHubCommitter.

        Args:
            repo_url: GitHub repository URL (defaults to configured repo)
            local_path: Local path to the git repository
        """
        self.repo_url = repo_url or self.REPO_URL
        self.local_path = Path(local_path)
        self.token = os.environ.get("GITHUB_TOKEN")

    def _run_git(self, *args, check: bool = True) -> subprocess.CompletedProcess:
        """Run a git command in the local repository."""
        cmd = ["git", "-C", str(self.local_path)] + list(args)
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=check
            )
            return result
        except subprocess.CalledProcessError as e:
            print(f"Git command failed: {' '.join(cmd)}")
            print(f"Error: {e.stderr}")
            raise

    def _get_authenticated_url(self) -> str:
        """Get the repository URL with token authentication."""
        if not self.token:
            raise ValueError("GITHUB_TOKEN environment variable not set")

        # Convert https://github.com/user/repo to https://token@github.com/user/repo
        if self.repo_url.startswith("https://"):
            return self.repo_url.replace("https://", f"https://{self.token}@")
        return self.repo_url

    def is_repo_initialized(self) -> bool:
        """Check if git repository is already initialized."""
        git_dir = self.local_path / ".git"
        return git_dir.exists()

    def setup_repo(self) -> bool:
        """
        Initialize git repo and set remote (one-time setup).

        Returns:
            True if setup successful, False otherwise
        """
        if not self.token:
            print("Error: GITHUB_TOKEN environment variable not set")
            print("Please set it with: set GITHUB_TOKEN=ghp_your_token_here")
            return False

        try:
            # Initialize git repo if not already
            if not self.is_repo_initialized():
                print("Initializing git repository...")
                self._run_git("init")

            # Check if remote exists
            result = self._run_git("remote", "-v", check=False)

            if "origin" in result.stdout:
                print("Updating existing remote 'origin'...")
                self._run_git("remote", "set-url", "origin", self._get_authenticated_url())
            else:
                print("Adding remote 'origin'...")
                self._run_git("remote", "add", "origin", self._get_authenticated_url())

            # Configure git user (use generic values if not set)
            result = self._run_git("config", "user.email", check=False)
            if not result.stdout.strip():
                self._run_git("config", "user.email", "nhl-predictions@auto.commit")
                self._run_git("config", "user.name", "NHL Predictions Bot")

            # Set default branch to main
            self._run_git("config", "init.defaultBranch", "main")

            print(f"Repository configured for: {self.repo_url}")
            return True

        except Exception as e:
            print(f"Setup failed: {e}")
            return False

    def commit_and_push(self, files: List[str], message: str) -> bool:
        """
        Add, commit, and push files to GitHub.

        Args:
            files: List of file paths to commit
            message: Commit message

        Returns:
            True if successful, False otherwise
        """
        if not self.token:
            print("Warning: GITHUB_TOKEN not set, skipping commit")
            return False

        try:
            # Ensure repo is set up
            if not self.is_repo_initialized():
                print("Repository not initialized. Run setup_github.py first.")
                return False

            # Update remote URL with token (in case it changed)
            self._run_git("remote", "set-url", "origin", self._get_authenticated_url())

            # Add files
            for file in files:
                self._run_git("add", file)

            # Check if there are changes to commit
            result = self._run_git("status", "--porcelain", check=False)
            if not result.stdout.strip():
                print("No changes to commit")
                return True

            # Commit
            self._run_git("commit", "-m", message)

            # Push (try main first, then master)
            try:
                self._run_git("push", "-u", "origin", "main")
            except subprocess.CalledProcessError:
                # Try creating main branch if it doesn't exist
                try:
                    self._run_git("branch", "-M", "main")
                    self._run_git("push", "-u", "origin", "main")
                except subprocess.CalledProcessError:
                    self._run_git("push", "-u", "origin", "master")

            print(f"Successfully pushed to {self.repo_url}")
            return True

        except Exception as e:
            print(f"Commit/push failed: {e}")
            return False

    def _regenerate_readme(self, game_date: str) -> bool:
        """Regenerate the predictions README with latest data."""
        try:
            from generate_readme import generate_predictions_readme
            print(f"Regenerating README for {game_date}...")
            generate_predictions_readme(game_date)
            return True
        except ImportError:
            print("Warning: generate_readme.py not found, skipping README regeneration")
            return False
        except Exception as e:
            print(f"Warning: Failed to regenerate README: {e}")
            return False

    def commit_results(self, game_date: str, predictions_data: Dict[str, Any],
                       filename: str = None) -> bool:
        """
        Commit prediction results with a formatted message.

        Args:
            game_date: Date of the predictions (YYYY-MM-DD)
            predictions_data: Dictionary containing prediction results
            filename: Path to the predictions file

        Returns:
            True if successful, False otherwise
        """
        # Regenerate the README with latest predictions and variance data
        self._regenerate_readme(game_date)

        # Calculate stats from predictions data
        all_predictions = predictions_data.get("all_predictions", [])

        if not all_predictions:
            print("No predictions to commit")
            return False

        # Count predictions with actual results
        evaluated = [p for p in all_predictions if p.get("actual_shots") is not None]

        if not evaluated:
            print("No evaluated predictions to commit")
            return False

        # Calculate metrics
        total = len(evaluated)
        errors = [abs(p.get("prediction_error", 0)) for p in evaluated if p.get("prediction_error") is not None]
        mae = sum(errors) / len(errors) if errors else 0

        # 2+ shots accuracy
        hit_2plus = sum(1 for p in evaluated if p.get("hit_2plus"))
        pred_2plus = sum(1 for p in evaluated if p.get("prob_2plus", 0) >= 0.5)
        accuracy_2plus = (hit_2plus / pred_2plus * 100) if pred_2plus > 0 else 0

        # Find best and worst predictions
        sorted_by_error = sorted(evaluated, key=lambda x: abs(x.get("prediction_error", 0)))
        best = sorted_by_error[0] if sorted_by_error else None
        worst = sorted_by_error[-1] if sorted_by_error else None

        # Build commit message
        message_lines = [
            f"[Results] {game_date}: MAE {mae:.2f}, {accuracy_2plus:.0f}% accuracy (2+ shots)",
            "",
            f"- {total} predictions evaluated"
        ]

        if best:
            message_lines.append(
                f"- Best: {best['player_name']} ({best['expected_shots']:.2f} exp -> {best['actual_shots']} actual)"
            )

        if worst and worst != best:
            message_lines.append(
                f"- Worst: {worst['player_name']} ({worst['expected_shots']:.2f} exp -> {worst['actual_shots']} actual)"
            )

        message = "\n".join(message_lines)

        # Determine files to commit
        if filename is None:
            filename = f"predictions/predictions_{game_date}.json"

        # Include both the JSON data and the README
        files_to_commit = [filename, "predictions/README.md"]

        return self.commit_and_push(files_to_commit, message)


def test_connection():
    """Test GitHub connection with current token."""
    committer = GitHubCommitter()

    if not committer.token:
        print("GITHUB_TOKEN not set!")
        print("Set it with: set GITHUB_TOKEN=ghp_your_token_here")
        return False

    print(f"Token found: {committer.token[:10]}...")
    print(f"Repository: {committer.repo_url}")

    # Test by checking if we can access the repo
    try:
        import urllib.request
        url = f"https://api.github.com/repos/dunnowhataliastouse/damnresultskek"
        req = urllib.request.Request(url, headers={
            "Authorization": f"token {committer.token}",
            "Accept": "application/vnd.github.v3+json"
        })
        with urllib.request.urlopen(req) as response:
            print("Connection successful!")
            return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


if __name__ == "__main__":
    print("Testing GitHub connection...")
    test_connection()
