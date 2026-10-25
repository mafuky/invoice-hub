.PHONY: up down ps tunnel tunnel-stop dev-check

up:
	@./scripts/deploy.sh up

down:
	@./scripts/deploy.sh down

ps:
	@./scripts/deploy.sh ps

tunnel:
	@./scripts/tunnel.sh start

tunnel-stop:
	@./scripts/tunnel.sh stop

dev-check:
	@./scripts/dev-check.sh
