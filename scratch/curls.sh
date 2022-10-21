curl -H 'Authorization: Bearer Kn7teXpj6ujOHznF2u4dpbLiwQWyqh6p' -d '{}' localhost:9000/v1/transfers

curl -d '{"source_host": "172.31.14.118", "source_port": 1433, "source_username": "sa", "source_password": "Mypass123", "source_db_name": "test", "target_account_id": "paccar", "aws_config_region": "us-west-2", "aws_config_s3_bucket": "s3-db-sql-server-backup-restore", "aws_config_s3_dir": "test-dir-3", "target_db_name": "RAW_TEST_DB", "target_private_key_location": "/home/ubuntu/.ssh/new_rsa.pem", "target_role": "SVC_ITD_03968_SQLPIPE_TEST_ROLE", "target_warehouse": "ITD_REN_03_968_SQLPIPE_TEST_SMALL_WH", "target_username": "n-ITD_03968_SQLPIPE_TEST_SVC", "target_aws_region": "us-west-2", "target_file_format_name": "sqlpipe_csv", "aws_config_key": "ASIA54ZDFHWQZ4725WGW", "aws_config_secret": "eZ5pPdcQRuDcI25WYKuWCHBZhFS+Au0yK/MR8JFI", "aws_config_token": "IQoJb3JpZ2luX2VjEIj//////////wEaCXVzLXdlc3QtMiJHMEUCIQCKHhhycwIfuqvRa6bue6IAhBSNB/4Xxr//2dkxC1VcjQIgAWaNjNK5hflmMPTkI74uKiTO2lb0+NZdsEgOhS4aCGkqvAMIYRADGgw5NTUxNjcwOTYyMjUiDAq9Ubnkiv+b+YrRWSqZA/uURa1S1CmK6ohXkzvcjCI6IJ1s3yhaB5RYWGWmof+65VqG9Ti+4H3rb1iOQjqgQougfTXQD1sPmUNGvl85zl+55gXpjAZs7OzR1meq12+Xmdk5QUHhIWSTTjGyG6jJE8aMc1YBM3RxO0fY9kYNG1w193JdwQv+1m5DEdOeTQqp71wCopIzF5sXhEQrJtiD6yJ0RhMp7RC4xF+a6A+XQ//O/+DgpTTz1v2m0wufP4d5pRZkxa3jTqdUG/R1/OhylZPnnhTVuKMDTPK+tNSkY484Qdju0q70lhPyGOVeV7e2l4mrgSYPOjwaJPBH9Ibn8XKKjhobbbvnj8lrJ5SOP8R9alQrfKVDxQaL+trXK2EkoE+Xz0y9hs0WNUhY2AxwjpbqpRR7givmvyIqJKiYq0HkpaI+6WAgMehba//4XbcBWOPe/ErhHgEyv015cpA1JynI8ny1fzA7OfvNezpdUqiYvOFASrJvmvYI22zPnCX99Jm/taPk/OnBVnvuWfTacDF+Ex9Trqsz7kCeaXZyfXkXaijmAwaNAMAwh9vFmgY6pgF5Bjt6Qjma42PCQOxJr+bc8KycL3j3elMD6iqfoM3ABxaqR/xMjkQv6eC/zT+TDP7VRX0G0nnFKUN+2lnxk5tMoen9VX5/S5EYYW2TYf10YBR5rcenwEEHxgEBXvoPIV+7+EhjMVYDZyqe2BXtVShTGVk1NH0JFihhHdhaFalABYuO2ZczPr/BiAfS5t+FZF6Mc5mFg03BY1Vc4W/2Cp8LYoBB3/wp"}' localhost:9000/v1/transfers

            "payload": {
                "source_host": "172.31.14.118",
                "source_port": 1433,
                "source_username": "sa",
                "source_password": "Mypass123",
                "source_db_name": "test",
                "target_account_id": "paccar",
                "aws_config_region": "us-west-2",
                "aws_config_s3_bucket": "s3-db-sql-server-backup-restore",
                "aws_config_s3_dir": "test-dir-3",
                "target_db_name": "RAW_TEST_DB",
                "target_private_key_location": "/home/ubuntu/.ssh/new_rsa.pem",
                "target_role": "SVC_ITD_03968_SQLPIPE_TEST_ROLE",
                "target_warehouse": "ITD_REN_03_968_SQLPIPE_TEST_SMALL_WH",
                "target_username": "n-ITD_03968_SQLPIPE_TEST_SVC",
                "target_aws_region": "us-west-2",
                "target_file_format_name": "sqlpipe_csv",
                "aws_config_key": "ASIA54ZDFHWQZ4725WGW",
                "aws_config_secret": "eZ5pPdcQRuDcI25WYKuWCHBZhFS+Au0yK/MR8JFI",
                "aws_config_token": "IQoJb3JpZ2luX2VjEIj//////////wEaCXVzLXdlc3QtMiJHMEUCIQCKHhhycwIfuqvRa6bue6IAhBSNB/4Xxr//2dkxC1VcjQIgAWaNjNK5hflmMPTkI74uKiTO2lb0+NZdsEgOhS4aCGkqvAMIYRADGgw5NTUxNjcwOTYyMjUiDAq9Ubnkiv+b+YrRWSqZA/uURa1S1CmK6ohXkzvcjCI6IJ1s3yhaB5RYWGWmof+65VqG9Ti+4H3rb1iOQjqgQougfTXQD1sPmUNGvl85zl+55gXpjAZs7OzR1meq12+Xmdk5QUHhIWSTTjGyG6jJE8aMc1YBM3RxO0fY9kYNG1w193JdwQv+1m5DEdOeTQqp71wCopIzF5sXhEQrJtiD6yJ0RhMp7RC4xF+a6A+XQ//O/+DgpTTz1v2m0wufP4d5pRZkxa3jTqdUG/R1/OhylZPnnhTVuKMDTPK+tNSkY484Qdju0q70lhPyGOVeV7e2l4mrgSYPOjwaJPBH9Ibn8XKKjhobbbvnj8lrJ5SOP8R9alQrfKVDxQaL+trXK2EkoE+Xz0y9hs0WNUhY2AxwjpbqpRR7givmvyIqJKiYq0HkpaI+6WAgMehba//4XbcBWOPe/ErhHgEyv015cpA1JynI8ny1fzA7OfvNezpdUqiYvOFASrJvmvYI22zPnCX99Jm/taPk/OnBVnvuWfTacDF+Ex9Trqsz7kCeaXZyfXkXaijmAwaNAMAwh9vFmgY6pgF5Bjt6Qjma42PCQOxJr+bc8KycL3j3elMD6iqfoM3ABxaqR/xMjkQv6eC/zT+TDP7VRX0G0nnFKUN+2lnxk5tMoen9VX5/S5EYYW2TYf10YBR5rcenwEEHxgEBXvoPIV+7+EhjMVYDZyqe2BXtVShTGVk1NH0JFihhHdhaFalABYuO2ZczPr/BiAfS5t+FZF6Mc5mFg03BY1Vc4W/2Cp8LYoBB3/wp",
            }