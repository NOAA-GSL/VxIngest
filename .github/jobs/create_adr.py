#!/usr/bin/env python3
"""
Script to create ADR (Architecture Decision Record) files from GitHub issues.
Can be run locally or in GitHub Actions.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class GitHubAPI:
    """Wrapper for GitHub API calls using gh CLI."""

    def get_issue(self, repo: str, issue_number: int) -> dict[str, str]:
        """Fetch issue data from GitHub API."""
        try:
            result = subprocess.run(
                ["gh", "api", f"repos/{repo}/issues/{issue_number}"],
                capture_output=True,
                text=True,
                check=True,
            )
            issue = json.loads(result.stdout)
            minimal_issue = {}
            minimal_issue["title"] = issue["title"]
            minimal_issue["body"] = issue["body"]
            return minimal_issue
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to fetch issue {issue_number} from {repo}: {e.stderr}"
            ) from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse GitHub API response: {e}") from e
        except KeyError as e:
            raise RuntimeError(f"Response object from GitHub for issue {repo}:{issue_number} didn't include title and/or body keys. Object was: {issue}") from e


class ADRGenerator:
    """Generates ADR files from GitHub issues."""

    def __init__(self, base_path: Path, adr_prefix: str = "adr"):
        self.adr_prefix = adr_prefix
        self.base_path = Path(base_path)

    def create_slug(self, title: str) -> str:
        """Create a URL-friendly slug from the title."""
        slug = title.lower().strip()
        slug = re.sub(r"[^\w\s-]", "", slug)  # Remove non-ascii characters
        slug = re.sub(
            r"[\s_-]+", "-", slug
        )  # Convert whitespace and underscore to dash
        slug = re.sub(r"^-+|-+$", "", slug)  # Remove leading/trailing dashes
        return slug

    def get_next_adr_number(self) -> str:
        """Get the next ADR number by examining existing files."""
        if not self.base_path.exists():
            return "0001"

        adr_files = list(self.base_path.glob(f"{self.adr_prefix}-*.md"))
        if not adr_files:
            return "0001"

        # Extract numbers from filenames
        numbers = []
        for file in adr_files:
            match = re.search(rf"{self.adr_prefix}-(\d+)-", file.name)
            if match:
                numbers.append(int(match.group(1)))

        if not numbers:
            return "0001"
            # raise RuntimeError(f"No files matching {self.adr_prefix}-(\d+) in {self.base_path}")

        next_number = max(numbers) + 1
        return f"{next_number:04d}"

    def generate_adr_content(
        self, title: str, body: str, date: Optional[str] = None
    ) -> str:
        """Generate the ADR markdown content."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        return f"""# {title}

Date: {date}

### Status

Accepted

{body}
"""

    def generate_adr_filepath(
        self, title: str, adr_number: Optional[str] = None
    ) -> Path:
        """Generate the ADR filepath"""
        if adr_number is None:
            adr_number = self.get_next_adr_number()

        slug = self.create_slug(title)
        filename = f"{self.adr_prefix}-{adr_number}-{slug}.md"
        filepath = self.base_path / filename

        return filepath

    def create_adr_file(
        self, filepath: Path, adr_content: str, adr_number: Optional[str] = None
    ) -> tuple[str, str]:
        """Create an ADR file from issue data."""
        # Create directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Write the file
        with filepath.open("w", encoding="utf-8") as f:
            f.write(adr_content)

        return str(filepath), adr_content


def main():
    """Main function for CLI usage."""
    parser = argparse.ArgumentParser(
        description="Create an ADR document from a GitHub issue"
    )
    parser.add_argument("--repo", required=True, help="GitHub repository (org/repo)")
    parser.add_argument("--issue", type=int, required=True, help="Issue number")
    parser.add_argument(
        "--adr-number", help="a 4 digit ADR number (auto-generated if not provided)"
    )
    parser.add_argument(
        "--adr-prefix", required=True, help="ADR filename prefix. Typically adr or sdr"
    )
    parser.add_argument("--base-path", required=True, help="Base path for ADR files")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print ADR content without creating file"
    )

    args = parser.parse_args()

    try:
        # Initialize GitHub API client
        github = GitHubAPI()

        # Fetch issue data
        issue_data = github.get_issue(args.repo, args.issue)

        # Initialize ADR generator
        adr_gen = ADRGenerator(adr_prefix=args.adr_prefix, base_path=args.base_path)

        # Get our data
        filepath = adr_gen.generate_adr_filepath(issue_data["title"], adr_number=args.adr_number)
        content = adr_gen.generate_adr_content(issue_data["title"], issue_data["body"])

        if args.dry_run:
            # Just print the content
            print(f"Filepath: {filepath}")
            print("=" * 50)
            print("ADR Content:")
            print("=" * 50)
            print(content)
        else:
            # Create the file
            filepath, content = adr_gen.create_adr_file(
                filepath, content, args.adr_number
            )
            print(f"Created ADR file: {filepath}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
