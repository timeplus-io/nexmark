NS=user-gang
NAME=ksqldb-server
POD=$(shell kubectl get pods --selector=io.kompose.service=$(NAME) -n $(NS) -o jsonpath="{.items[0].metadata.name}")


install:
	kubectl apply -f .

uninstall:
	kubectl delete deployment ksqldb-server -n $(NS)

client:
	kubectl exec -it $(POD) -c ksqldb-server -n $(NS) -- ksql

logs:
	kubectl logs $(POD) -n $(NS) -f
