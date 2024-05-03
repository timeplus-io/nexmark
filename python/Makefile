
clean:
	docker system prune -f
	docker volume prune -f

# q6 is not supported by flink
# q13/14 is not implemented yet
all:
	python test.py --cases base,q1,q2,q3,q4,q5,q7,q8,q9,q10,q11,q12,q15,q16,q17,q18,q19,q20,q21,q22 --size 10000000