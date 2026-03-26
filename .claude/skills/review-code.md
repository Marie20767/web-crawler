---
name: review-code
description: Reviews code for quality, bugs, or security issues. Use when the user asks to review, audit, or check code quality.
argument-hint: [path] [focus: quality|security|performance|structure]
allowed-tools: Read, Bash(rg:*), Glob
---

You will receive arguments in this format: `$ARGUMENTS`

Parse $ARGUMENTS as follows:
- The **first token** is the file or directory path to review (e.g. `./services/crawler`)
- The **second token** (optional) is the focus area: `quality`, `security`, or `performance`

Example inputs:
- `./services/crawler/consumer/consumer.go security` → review that file, focus on security
- `./services/crawler` → review that directory, general review
- `./services/crawler/consumer/consmer.go structure` → suggest better structure for that package

Start by reading the file or directory at the given path, then apply the relevant review below.

## Quality Review
- Check for readability and naming conventions
- Identify duplicated logic or opportunities to refactor
- Flag any functions that are too long or complex

## Security Review
- Look for exposed secrets or credentials
- Check for injection vulnerabilities (SQL, shell, etc.)
- Identify insecure handling of user input

## Performance Review
- Spot unnecessary loops or redundant computations
- Flag blocking operations that could be async
- Identify missing indexes or N+1 query patterns

## Structure Review
- Assess whether files and folders follow a clear, consistent convention
- Identify files that are doing too much and should be split up
- Flag misplaced files (e.g. business logic in a utils folder)
- Suggest a revised folder/file layout with a brief rationale for each change
- Focus on single responsibility principle for go packages

## Output Format
Summarise findings as:
1. **Critical** – must fix
2. **Suggestions** – worth improving

Always reference specific file names and line numbers where possible.
For structure suggestions, show a before/after directory tree.