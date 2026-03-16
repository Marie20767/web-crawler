up:
docker compose -f kafka/docker/docker-compose.yml -f producer/docker/docker-compose.yml up -d

down:
docker compose -f kafka/docker/docker-compose.yml -f producer/docker/docker-compose.yml down