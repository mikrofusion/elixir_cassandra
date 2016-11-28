# development container commands
start:
	docker-compose -p cassandra_dev up -d

logs:
	docker-compose -p cassandra_dev logs -f

shell:
	docker exec -it cassandradev_coordinator_1 bash

cqlsh:
	docker exec -i -t cassandradev_coordinator_1 sh -c cqlsh

status:
	docker exec -i -t cassandradev_coordinator_1 sh -c 'nodetool -u cassandra -pw cassandra status'

stop:
	docker-compose -p cassandra_dev down

init:
	docker exec -i -t cassandradev_coordinator_1 sh -c "cqlsh -e \"CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};\""

init-ci:
	cqlsh -e "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"

clean:
	docker volume rm `docker volume ls -q -f dangling=true`
