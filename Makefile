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

k8/start:
	minikube start --nodes=$(nodes) --driver=docker

k8/build:
	docker build -t url-crawler:latest -f services/crawler/docker/Dockerfile .
	docker build -t url-parser:latest -f services/parser/docker/Dockerfile .
	docker build -t url-initialiser:latest -f services/initialiser/docker/Dockerfile .

k8/apply:
	kubectl apply -f infra/k8s/ --recursive

k8/stop:
	minikube stop

k8/delete:
	minikube delete