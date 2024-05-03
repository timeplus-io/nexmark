NS=user-gang
NAME=nexmarkgen
POD=$(shell kubectl get pods --selector=app=$(NAME) -n $(NS) -o jsonpath="{.items[0].metadata.name}")

start:
	kubectl apply -f ./job.yaml

delete:
	kubectl delete job $(NAME) -n $(NS)

logs:
	kubectl logs $(POD) -n $(NS) -f