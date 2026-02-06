import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from create_adr import ADRGenerator, GitHubAPI


class TestADRGenerator:
    """Test cases for ADRGenerator class."""

    def test_create_slug(self, tmp_path):
        """Test slug creation from titles."""
        generator = ADRGenerator(base_path=tmp_path)

        test_cases = [
            ("Simple Title", "simple-title"),
            ("Title with Numbers 123", "title-with-numbers-123"),
            ("Title with Special Characters!@#", "title-with-special-characters"),
            ("Multiple   Spaces", "multiple-spaces"),
            ("Title_with_underscores", "title-with-underscores"),
            ("---Leading and Trailing---", "leading-and-trailing"),
            ("", ""),
            ("CamelCase Title", "camelcase-title"),
            ("Title with (parentheses)", "title-with-parentheses"),
        ]

        for title, expected in test_cases:
            assert generator.create_slug(title) == expected

    def test_get_next_adr_number_no_directory(self, tmp_path):
        """Test ADR number generation when directory doesn't exist."""
        generator = ADRGenerator(base_path=tmp_path / "nonexistent")
        assert generator.get_next_adr_number() == "0001"

    def test_get_next_adr_number_empty_directory(self, tmp_path):
        """Test ADR number generation with empty directory."""
        generator = ADRGenerator(base_path=tmp_path)
        assert generator.get_next_adr_number() == "0001"

    def test_get_next_adr_number_existing_files(self, tmp_path):
        """Test ADR number generation with existing files."""
        # Create some existing ADR files
        (tmp_path / "adr-0001-first.md").touch()
        (tmp_path / "adr-0003-third.md").touch()
        (tmp_path / "adr-0002-second.md").touch()
        (tmp_path / "other-file.md").touch()  # Should be ignored

        generator = ADRGenerator(base_path=tmp_path)
        assert generator.get_next_adr_number() == "0004"

    def test_get_next_adr_number_with_custom_prefix(self, tmp_path):
        """Test ADR number generation with custom prefix."""
        # Create some existing SDR files
        (tmp_path / "sdr-0001-first.md").touch()
        (tmp_path / "sdr-0003-third.md").touch()
        (tmp_path / "adr-0002-ignored.md").touch()  # Should be ignored

        generator = ADRGenerator(adr_prefix="sdr", base_path=tmp_path)
        assert generator.get_next_adr_number() == "0004"

    def test_get_next_adr_number_no_matching_files(self, tmp_path):
        """Test ADR number generation with no matching files."""
        # Create files that don't match the pattern
        (tmp_path / "other-file.md").touch()
        (tmp_path / "readme.md").touch()

        generator = ADRGenerator(base_path=tmp_path)
        assert generator.get_next_adr_number() == "0001"

    def test_generate_adr_content(self, tmp_path):
        """Test ADR content generation."""
        generator = ADRGenerator(tmp_path)

        title = "Test Decision"
        body = "This is the body of the decision.\n\nIt has multiple paragraphs."
        date = "2023-07-07"

        content = generator.generate_adr_content(title, body, date)

        assert "# Test Decision" in content
        assert "Date: 2023-07-07" in content
        assert "### Status" in content
        assert "Accepted" in content
        assert "This is the body of the decision." in content
        assert "It has multiple paragraphs." in content

    def test_generate_adr_content_with_default_date(self, tmp_path):
        """Test ADR content generation with default date."""
        generator = ADRGenerator(base_path=tmp_path)

        content = generator.generate_adr_content("Test", "Body")

        # Should contain today's date
        from datetime import datetime

        today = datetime.now().strftime("%Y-%m-%d")
        assert f"Date: {today}" in content

    def test_generate_adr_content_empty_body(self, tmp_path):
        """Test ADR content generation with empty body."""
        generator = ADRGenerator(base_path=tmp_path)

        content = generator.generate_adr_content("Test Title", "")

        assert "# Test Title" in content
        assert "### Status" in content
        assert "Accepted" in content
        # Empty body should still be included
        lines = content.split("\n")
        assert "" in lines  # Empty body line should exist

    def test_generate_adr_filepath(self, tmp_path):
        """Test ADR filepath generation."""
        generator = ADRGenerator(base_path=tmp_path)

        filepath = generator.generate_adr_filepath("Test Decision", "0005")

        assert str(filepath).endswith("adr-0005-test-decision.md")
        assert tmp_path in filepath.parents

    def test_generate_adr_filepath_with_custom_prefix(self, tmp_path):
        """Test ADR filepath generation with custom prefix."""
        generator = ADRGenerator(adr_prefix="sdr", base_path=tmp_path)

        filepath = generator.generate_adr_filepath("Test Decision", "0005")

        assert str(filepath).endswith("sdr-0005-test-decision.md")

    def test_generate_adr_filepath_auto_number(self, tmp_path):
        """Test ADR filepath generation with auto-generated number."""
        # Create existing file
        (tmp_path / "adr-0001-existing.md").touch()

        generator = ADRGenerator(base_path=tmp_path)
        filepath = generator.generate_adr_filepath("New Decision")

        assert str(filepath).endswith("adr-0002-new-decision.md")

    def test_create_adr_file(self, tmp_path):
        """Test creating ADR file."""
        generator = ADRGenerator(base_path=tmp_path)

        filepath = Path(tmp_path) / "adr-0001-test.md"
        content = "# Test Content\n\nThis is a test."

        result_filepath, result_content = generator.create_adr_file(filepath, content)

        # Check file was created
        assert Path(result_filepath).exists()
        assert result_filepath == str(filepath)
        assert result_content == content

        # Check file content
        with filepath.open() as f:
            file_content = f.read()

        assert file_content == content

    def test_create_adr_file_creates_directory(self, tmp_path):
        """Test that create_adr_file creates the directory if it doesn't exist."""
        nested_path = Path(tmp_path) / "nested" / "directory"
        generator = ADRGenerator(base_path=nested_path)

        filepath = nested_path / "adr-0001-test.md"
        content = "# Test Content"

        generator.create_adr_file(filepath, content)

        # Check directory was created
        assert nested_path.exists()
        assert nested_path.is_dir()

        # Check file was created
        assert filepath.exists()


class TestGitHubAPI:
    """Test cases for GitHubAPI class."""

    @patch("subprocess.run")
    def test_get_issue_success(self, mock_run):
        """Test successful issue fetching."""
        mock_response = {
            "title": "Test Issue",
            "body": "Test body content",
            "number": 123,
            "state": "closed",
            "labels": [{"name": "ADR: accepted"}],
        }

        mock_run.return_value = MagicMock(
            stdout=json.dumps(mock_response), stderr="", returncode=0
        )

        api = GitHubAPI()
        result = api.get_issue("owner/repo", 123)

        # Should return minimal issue data
        expected = {"title": "Test Issue", "body": "Test body content"}
        assert result == expected

        mock_run.assert_called_once_with(
            ["gh", "api", "repos/owner/repo/issues/123"],
            capture_output=True,
            text=True,
            check=True,
        )

    @patch("subprocess.run")
    def test_get_issue_subprocess_error(self, mock_run):
        """Test error handling for subprocess failure."""
        mock_run.side_effect = subprocess.CalledProcessError(
            1, ["gh", "api"], stderr="API error: Not found"
        )

        api = GitHubAPI()

        with pytest.raises(
            RuntimeError, match="Failed to fetch issue 123 from owner/repo"
        ):
            api.get_issue("owner/repo", 123)

    @patch("subprocess.run")
    def test_get_issue_json_error(self, mock_run):
        """Test error handling for invalid JSON response."""
        mock_run.return_value = MagicMock(
            stdout="invalid json response", stderr="", returncode=0
        )

        api = GitHubAPI()

        with pytest.raises(RuntimeError, match="Failed to parse GitHub API response"):
            api.get_issue("owner/repo", 123)

    @patch("subprocess.run")
    def test_get_issue_missing_fields(self, mock_run):
        """Test handling of missing title or body fields."""
        mock_response = {
            "number": 123,
            "state": "closed",
            # Missing title and body
        }

        mock_run.return_value = MagicMock(
            stdout=json.dumps(mock_response), stderr="", returncode=0
        )

        api = GitHubAPI()

        with pytest.raises(RuntimeError):
            api.get_issue("owner/repo", 123)


@pytest.fixture
def sample_issue_data():
    """Sample issue data for testing."""
    return {
        "title": "Adopt GraphQL for API",
        "body": "## Context\n\nWe need to improve our API performance.\n\n## Decision\n\nWe will adopt GraphQL.",
    }


class TestIntegration:
    """Integration tests for the complete workflow."""

    def test_full_workflow_adr_prefix(self, sample_issue_data, tmp_path):
        """Test the complete workflow with ADR prefix."""
        generator = ADRGenerator(adr_prefix="adr", base_path=tmp_path)

        # Generate filepath and content
        filepath = generator.generate_adr_filepath(sample_issue_data["title"])
        content = generator.generate_adr_content(
            sample_issue_data["title"], sample_issue_data["body"]
        )

        # Create the file
        result_filepath, result_content = generator.create_adr_file(filepath, content)

        # Verify file exists and has correct name
        assert Path(result_filepath).exists()
        assert "adr-0001-adopt-graphql-for-api.md" in result_filepath

        # Verify content structure
        assert "# Adopt GraphQL for API" in result_content
        assert "### Status" in result_content
        assert "Accepted" in result_content
        assert "We need to improve our API performance." in result_content
        assert "We will adopt GraphQL." in result_content

    def test_full_workflow_sdr_prefix(self, sample_issue_data, tmp_path):
        """Test the complete workflow with SDR prefix."""
        generator = ADRGenerator(adr_prefix="sdr", base_path=tmp_path)

        # Generate filepath and content
        filepath = generator.generate_adr_filepath(sample_issue_data["title"])
        content = generator.generate_adr_content(
            sample_issue_data["title"], sample_issue_data["body"]
        )

        # Create the file
        result_filepath, result_content = generator.create_adr_file(filepath, content)

        # Verify file exists and has correct name
        assert Path(result_filepath).exists()
        assert "sdr-0001-adopt-graphql-for-api.md" in result_filepath

    def test_sequential_adr_creation(self, tmp_path):
        """Test creating multiple ADRs in sequence."""
        generator = ADRGenerator(base_path=tmp_path)

        # Create first ADR
        filepath1 = generator.generate_adr_filepath("First Decision")
        content1 = generator.generate_adr_content("First Decision", "First body")
        generator.create_adr_file(filepath1, content1)

        # Create second ADR
        filepath2 = generator.generate_adr_filepath("Second Decision")
        content2 = generator.generate_adr_content("Second Decision", "Second body")
        generator.create_adr_file(filepath2, content2)

        # Verify both files exist with correct numbering
        assert "adr-0001-first-decision.md" in str(filepath1)
        assert "adr-0002-second-decision.md" in str(filepath2)
        assert Path(filepath1).exists()
        assert Path(filepath2).exists()

    def test_mixed_prefixes(self, tmp_path):
        """Test that different prefixes don't interfere with each other."""
        # Create an ADR file
        adr_generator = ADRGenerator(adr_prefix="adr", base_path=tmp_path)
        adr_filepath = adr_generator.generate_adr_filepath("ADR Decision")
        adr_content = adr_generator.generate_adr_content("ADR Decision", "ADR body")
        adr_generator.create_adr_file(adr_filepath, adr_content)

        # Create an SDR file - should start from 0001 since it has different prefix
        sdr_generator = ADRGenerator(adr_prefix="sdr", base_path=tmp_path)
        sdr_filepath = sdr_generator.generate_adr_filepath("SDR Decision")
        sdr_content = sdr_generator.generate_adr_content("SDR Decision", "SDR body")
        sdr_generator.create_adr_file(sdr_filepath, sdr_content)

        # Verify both files exist with correct numbering
        assert "adr-0001-adr-decision.md" in str(adr_filepath)
        assert "sdr-0001-sdr-decision.md" in str(sdr_filepath)
        assert Path(adr_filepath).exists()
        assert Path(sdr_filepath).exists()

        # Create another ADR - should be numbered 0002
        adr_filepath2 = adr_generator.generate_adr_filepath("Second ADR")
        adr_content2 = adr_generator.generate_adr_content("Second ADR", "Second body")
        adr_generator.create_adr_file(adr_filepath2, adr_content2)

        assert "adr-0002-second-adr.md" in str(adr_filepath2)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_title_slug(self, tmp_path):
        """Test slug generation with empty title."""
        generator = ADRGenerator(base_path=tmp_path)
        assert generator.create_slug("") == ""
        assert generator.create_slug("   ") == ""
        assert generator.create_slug("---") == ""

    def test_very_long_title(self, tmp_path):
        """Test handling of very long titles."""
        generator = ADRGenerator(base_path=tmp_path)
        long_title = "A" * 200  # Very long title
        slug = generator.create_slug(long_title)
        assert slug == "a" * 200

    def test_numbers_in_adr_filename_pattern(self, tmp_path):
        """Test that the regex correctly handles various number patterns."""
        base_path = Path(tmp_path)

        # Create files with various number patterns
        (base_path / "adr-0001-test.md").touch()
        (base_path / "adr-0010-test.md").touch()
        (base_path / "adr-0100-test.md").touch()
        (base_path / "adr-1000-test.md").touch()
        (base_path / "adr-01-invalid.md").touch()  # Should work but not recommended

        generator = ADRGenerator(base_path=tmp_path)
        next_number = generator.get_next_adr_number()

        assert next_number == "1001"
