up:
	aws sso login --profile terraform
	docker compose up -d

build:
	aws sso login --profile terraform
	docker compose up --build -d

down:
	docker compose down