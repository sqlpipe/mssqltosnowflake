include .envrc

## run/sqlpipe: run the cmd/sqlpipe application
.PHONY: run/sqlpipe
run/sqlpipe:
	go run ./cmd/sqlpipe

## vendor: tidy and vendor dependencies
.PHONY: vendor
vendor:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy
	go mod verify
	@echo 'Vendoring dependencies...'
	go mod vendor

## build/sqlpipe: build the cmd/sqlpipe application
.PHONY: build/sqlpipe
build/sqlpipe:
	@echo 'Building cmd/sqlpipe...'
	go build -ldflags="-s" -o=./bin/sqlpipe ./cmd/sqlpipe

## transfer: run a sample transfer
.PHONY: transfer
transfer:
	curl -d '{"aws_config_key": "${AWS_CONFIG_KEY}", "aws_config_s3_bucket": "${AWS_CONFIG_S3_BUCKET}", "aws_config_s3_dir": "${AWS_CONFIG_S3_DIR}", "aws_config_region": "${AWS_CONFIG_REGION}", "aws_config_secret": "${AWS_CONFIG_SECRET}", "source_db_name": "${SOURCE_DB_NAME}", "source_host": "${SOURCE_HOST}", "source_port": ${SOURCE_PORT}, "source_username": "${SOURCE_USERNAME}", "source_password": "${SOURCE_PASSWORD}", "target_account_id": "${TARGET_ACCOUNT_ID}", "target_username": "${TARGET_USERNAME}", "target_db_name": "${TARGET_DB_NAME}", "target_aws_region": "${TARGET_AWS_REGION}", "target_private_key_location": "${TARGET_PRIVATE_KEY_LOCATION}", "target_role": "${TARGET_ROLE}", "target_warehouse": "${TARGET_WAREHOUSE}", "target_file_format_name": "${TARGET_FILE_FORMAT_NAME}"}' localhost:9000/v1/transfers