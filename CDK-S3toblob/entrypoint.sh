#!/bin/bash

echo "export AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" >> /root/.profile
json=$(curl "http://169.254.170.2${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}")

export AWS_ACCESS_KEY_ID=$(echo "$json" | jq -r '.AccessKeyId') 
export AWS_SECRET_ACCESS_KEY=$(echo "$json" | jq -r '.SecretAccessKey') 
export AWS_SESSION_TOKEN=$(echo "$json" | jq -r '.Token') 

azcopy copy "${S3_SOURCE}" \
"${AZURE_BLOB_URL}?${SAS_TOKEN}" \
--recursive=true