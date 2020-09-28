AWS_READY=^Ready\.$

python-image:
	@{ \
  		cd ./resources/containers/python; \
  		docker build --tag dwp-python:latest .; \
	}

aws-init-image: python-image
	docker-compose build aws-init

aws: ## Bring up localstack container.
	docker-compose up -d aws
	@{ \
		while ! docker logs aws 2> /dev/null | grep -q $(AWS_READY); do \
			echo Waiting for aws.; \
			sleep 2; \
		done; \
	}
	echo aws container is up.

aws-init: aws aws-init-image ## Create buckets and objects needed in s3 for the integration tests
	docker-compose up aws-init

hbase: ## Bring up hbase
	docker-compose up -d hbase
	@{ \
		echo Waiting for HBase.; \
		while ! docker logs hbase 2>&1 | grep "Master has completed initialization" ; do \
			sleep 2; \
			echo Waiting for HBase.; \
		done; \
	}
	echo HBase up.

mysql:
	docker-compose up -d mysql
	@{ \
		while ! docker logs mysql 2>&1 | grep "^Version" | grep 3306; do \
			echo Waiting for MySQL.; \
			sleep 2; \
		done; \
	}
	echo MySQL up.

mysql-init: mysql
	docker exec -i mysql mysql --user=root --password=password metadatastore  < ./resources/containers/mysql/create_table.sql
	docker exec -i mysql mysql --user=root --password=password metadatastore  < ./resources/containers/mysql/grant_user.sql

start-services: hbase mysql-init aws-init

stop-services:
	docker ps -a | awk '{ print $$1 }' | fgrep --color=auto -v CONTAINER | xargs -r docker stop | xargs -r docker rm

restart-services: stop-services start-services
