
.PHONY: clean all

clean:
	docker system prune -f
	docker volume prune -f

# q6 is not supported by flink
# q13
# q14 will timeout on q14
all:
	python nexmark.py --cases base,q0,q1,q2,q3,q4,q5,q7,q8,q9,q10,q11,q12,q15,q16,q17,q18,q19,q20,q21,q22 --platforms flink,timeplus,ksqldb --data-size 10000000

# run timeplus locally as analysis service
timeplus:
	docker run -d -p 8000:8000 -p 3218:3218 -p 8123:8123 -p 8463:8463 timeplus/timeplus-enterprise:2.9.0-rc.5