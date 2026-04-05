#!/usr/bin/env bash
set -euo pipefail

# PR Summary Report Generator
# Uses GitHub CLI to fetch merged PRs and Claude Code to categorize them.
#
# Usage:
#   ./scripts/pr-summary.sh                      # PRs merged since last tag
#   ./scripts/pr-summary.sh --since 2024-01-01   # PRs merged since date
#   ./scripts/pr-summary.sh --last 30            # PRs merged in the last 30 days
#   ./scripts/pr-summary.sh --between v1.0..v1.1 # PRs between two tags/refs
#   ./scripts/pr-summary.sh --format slack        # Slack mrkdwn output

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"
BASE_BRANCH="${BASE_BRANCH:-master}"
OUTPUT_FILE=""
FORMAT="markdown"
SINCE=""
LAST=""
BETWEEN=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Generate a categorized summary of merged pull requests.

Options:
  --since DATE        PRs merged since DATE (YYYY-MM-DD)
  --last N            PRs merged in the last N days (default: all since last tag)
  --between REF..REF  PRs between two git refs (tags, commits, branches)
  --format FORMAT     Output format: markdown (default) or slack
  --repo OWNER/REPO   GitHub repo (default: current repo)
  --branch BRANCH     Base branch (default: master)
  --output FILE       Write report to FILE (default: stdout only)
  -h, --help          Show this help
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)   SINCE="$2"; shift 2 ;;
    --last)    LAST="$2"; shift 2 ;;
    --between) BETWEEN="$2"; shift 2 ;;
    --format)  FORMAT="$2"; shift 2 ;;
    --repo)    REPO="$2"; shift 2 ;;
    --branch)  BASE_BRANCH="$2"; shift 2 ;;
    --output)  OUTPUT_FILE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

if [[ "$FORMAT" != "markdown" && "$FORMAT" != "slack" ]]; then
  echo "Error: --format must be 'markdown' or 'slack'" >&2
  exit 1
fi

# Check dependencies
for cmd in gh claude; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: '$cmd' is not installed or not in PATH." >&2
    exit 1
  fi
done

# All progress messages go to stderr so stdout is clean for piping
echo "Fetching merged PRs for $REPO ($BASE_BRANCH)..." >&2

# Build gh search query
GH_ARGS=(pr list --repo "$REPO" --base "$BASE_BRANCH" --state merged --json number,title,url,labels,author,mergedAt,body --limit 300)

if [[ -n "$SINCE" ]]; then
  PR_JSON=$(gh "${GH_ARGS[@]}" | jq --arg since "$SINCE" '[.[] | select(.mergedAt >= $since)]')
elif [[ -n "$LAST" ]]; then
  # Calculate date N days ago (compatible with both macOS and Linux)
  if date -v-1d &>/dev/null 2>&1; then
    SINCE_DATE=$(date -v-"${LAST}"d +%Y-%m-%d)
  else
    SINCE_DATE=$(date -d "$LAST days ago" +%Y-%m-%d)
  fi
  echo "Using PRs merged in the last $LAST days (since $SINCE_DATE)" >&2
  PR_JSON=$(gh "${GH_ARGS[@]}" | jq --arg since "$SINCE_DATE" '[.[] | select(.mergedAt >= $since)]')
elif [[ -n "$BETWEEN" ]]; then
  REF_FROM="${BETWEEN%..*}"
  REF_TO="${BETWEEN#*..}"
  SINCE_DATE=$(gh api "repos/$REPO/commits/$REF_FROM" --jq .commit.committer.date | cut -dT -f1)
  UNTIL_DATE=$(gh api "repos/$REPO/commits/$REF_TO" --jq .commit.committer.date | cut -dT -f1)
  PR_JSON=$(gh "${GH_ARGS[@]}" | jq --arg since "$SINCE_DATE" --arg until "$UNTIL_DATE" \
    '[.[] | select(.mergedAt >= $since and .mergedAt <= $until)]')
else
  # Default: since last tag
  LAST_TAG=$(gh api "repos/$REPO/tags?per_page=1" --jq '.[0].name' 2>/dev/null || echo "")
  if [[ -n "$LAST_TAG" ]]; then
    SINCE_DATE=$(gh api "repos/$REPO/commits/$LAST_TAG" --jq .commit.committer.date | cut -dT -f1)
    echo "Using PRs merged since last tag: $LAST_TAG ($SINCE_DATE)" >&2
    PR_JSON=$(gh "${GH_ARGS[@]}" | jq --arg since "$SINCE_DATE" '[.[] | select(.mergedAt >= $since)]')
  else
    echo "No tags found, fetching last 100 merged PRs" >&2
    GH_ARGS+=(--limit 100)
    PR_JSON=$(gh "${GH_ARGS[@]}")
  fi
fi

PR_COUNT=$(echo "$PR_JSON" | jq 'length')
echo "Found $PR_COUNT merged PRs." >&2

if [[ "$PR_COUNT" -eq 0 ]]; then
  echo "No PRs to summarize." >&2
  exit 0
fi

# Prepare a compact version for Claude (trim long bodies)
COMPACT_JSON=$(echo "$PR_JSON" | jq '[.[] | {
  number,
  title,
  url,
  labels: [.labels[].name],
  author: .author.login,
  mergedAt: (.mergedAt | split("T")[0]),
  body: (.body // "" | if length > 500 then .[:500] + "..." else . end)
}]')

echo "Sending to Claude Code for categorization..." >&2

if [[ "$FORMAT" == "slack" ]]; then
  FORMAT_INSTRUCTIONS="## Output format: Slack mrkdwn
- Use *bold* for section headers (not ### markdown headings)
- Links: <URL|#NUMBER> (Slack format, not markdown)
- Use bullet lists with plain \`-\` or \`•\`
- No markdown headings (# ## ###) — Slack doesn't render them
- Keep the entire message under 3000 characters (Slack block limit)
- If the report would exceed 3000 chars, summarize the Maintenance section as a count instead of listing each PR"
else
  FORMAT_INSTRUCTIONS="## Output format: GitHub Markdown
- Each PR entry: \`- TITLE [#NUMBER](URL) @AUTHOR\`
- Use ### for section headings"
fi

PROMPT="You are generating a release notes summary for the GitHub repository $REPO.

Below is a JSON array of merged pull requests. Categorize each PR and produce a report.

## Categories (use exactly these headings, skip empty sections):
- Breaking Changes
- New Features
- Improvements
- Bug Fixes
- Maintenance
- Other

$FORMAT_INSTRUCTIONS

## Rules:
- Within each section, group related PRs together
- If a PR has a label like \"bug\", \"feature\", \"enhancement\", \"breaking-change\", \"dependencies\", \"documentation\", prefer the label for categorization
- Otherwise infer from title and body
- Add a one-line summary at the top with the date range and PR count
- Keep it concise — no per-PR descriptions unless the title is unclear
- Output ONLY the report, nothing else

Here are the PRs:

$COMPACT_JSON"

# Use Claude Code in non-interactive mode to generate the report
REPORT=$(echo "$PROMPT" | claude -p)

# Write to file if --output was specified
if [[ -n "$OUTPUT_FILE" ]]; then
  echo "$REPORT" > "$OUTPUT_FILE"
  echo "Report written to $OUTPUT_FILE" >&2
fi

# Set GitHub Actions output if running in CI
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "report<<REPORT_EOF"
    echo "$REPORT"
    echo "REPORT_EOF"
  } >> "$GITHUB_OUTPUT"
  echo "Report set as GitHub Actions step output 'report'" >&2
fi

# Always print to stdout
echo "$REPORT"
