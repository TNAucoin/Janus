.PHONY: up down build logs help rebuild

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

up: ## Start all services
	@docker-compose up

down: ## Stop and remove containers, networks, images, and volumes
	@docker-compose down

build: ## Build services images
	@docker-compose build

rebuild: ## Stop services, rebuild images, and start the services
	make down
	make build
	make up

logs: ## Follow log output for running containers
	@docker-compose logs -f