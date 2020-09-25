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
