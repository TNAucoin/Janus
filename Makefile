.PHONY: all-up all-down all-build logs help all-rebuild services-up services-stop services-build app-up app-down app-build app-rebuild

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

all-up: ## Start all services
	@docker-compose up

all-down: ## Stop and remove containers, networks, images, and volumes
	@docker-compose down

all-build: ## Build services images
	@docker-compose build

all-rebuild: ## Stop services, rebuild images, and start the services
	make down
	make build
	make up

services-up: ## Start backend services
	@docker-compose up -d ddb-local mq

services-stop: ## Stop backend services
	@docker-compose stop ddb-local mq

services-build: ## Build backend services
	@docker-compose build ddb-local mq

app-up: ## Start the application
	@docker-compose up janus

app-down: ## Stop the application
	@docker-compose stop janus

app-build: ## Build the application
	@docker-compose build janus

app-rebuild: ## Stop, Build, and Restart the application
	make app-down
	make app-build
	make app-up

logs: ## Follow log output for running containers
	@docker-compose logs -f