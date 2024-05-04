
start:
	docker-compose up

topic:
	docker exec -it kafka rpk topic create nexmark-auction nexmark-person nexmark-bid nexmark-events

clean:
	docker system prune -f
	docker volume prune -f


test:
	${FLINK_HOME}/bin/sql-client.sh embedded -l ${SQL_CLIENT_HOME}/lib -f /home/scripts/q0.sql