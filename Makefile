claude: 
	claude --append-system-prompt "Before doing anything else, run: cat .claude/REFLECTION_NEEDED.md 2>/dev/null — if the output is not empty, append any architectural decisions to CLAUDE.md under '## Architecture', then run: rm .claude/REFLECTION_NEEDED.md"

up:
	aws sso login --profile terraform
	docker compose up -d

build:
	aws sso login --profile terraform
	docker compose up --build -d

down:
	docker compose down

down/volumes:
	docker compose down -v