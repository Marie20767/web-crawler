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

k8s/setup:
	minikube start --nodes=$(nodes) --driver=docker
	minikube addons enable metrics-server
	$(MAKE) k8s/build
	$(MAKE) k8s/load
	$(MAKE) k8s/apply

k8s/build:
	docker build -t url-crawler:latest -f services/crawler/docker/Dockerfile .
# 	docker build -t url-parser:latest -f services/parser/docker/Dockerfile .
	docker build -t url-initialiser:latest -f services/initialiser/docker/Dockerfile .

k8s/load:
	minikube image load url-crawler:latest
# 	minikube image load url-parser:latest
	minikube image load url-initialiser:latest

k8s/apply:
	kubectl create secret generic aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=$(terraform output -raw web_crawler_access_key_id) \
  --from-literal=AWS_SECRET_ACCESS_KEY=$(terraform output -raw web_crawler_secret_access_key)
	kubectl apply -f infra/k8s/ --recursive

k8s/stop:
	minikube stop

k8s/delete:
	minikube delete