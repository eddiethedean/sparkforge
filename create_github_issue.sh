#!/bin/bash
# Script to create GitHub issue using GitHub CLI (gh)
# Requires: gh CLI installed and authenticated

REPO="eddiethedean/sparkless"
TITLE="DataFrame.filter() returns 0 rows on tables created with saveAsTable() using complex schemas"

# Read the issue body from file
BODY_FILE="GITHUB_ISSUE_CONTENT.md"

if [ ! -f "$BODY_FILE" ]; then
    echo "Error: $BODY_FILE not found"
    exit 1
fi

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed"
    echo "Install it from: https://cli.github.com/"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo "Error: Not authenticated with GitHub CLI"
    echo "Run: gh auth login"
    exit 1
fi

# Create the issue
echo "Creating GitHub issue in $REPO..."
echo "Title: $TITLE"
echo ""

gh issue create \
    --repo "$REPO" \
    --title "$TITLE" \
    --body-file "$BODY_FILE" \
    --label "bug"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Issue created successfully!"
    echo "View it at: https://github.com/$REPO/issues"
else
    echo ""
    echo "❌ Failed to create issue"
    exit 1
fi

