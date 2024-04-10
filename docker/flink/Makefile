
start:
	docker-compose up

topic:
	docker exec -it kafka rpk topic create nexmark-auction nexmark-person nexmark-bid nexmark-events

gen:
	docker-compose start generator