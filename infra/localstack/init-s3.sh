#!/bin/bash
set -euo pipefail

echo "Ensuring S3 bucket exists: ${BOOTSTRAP_BUCKET_NAME}"

if awslocal s3api head-bucket --bucket "${BOOTSTRAP_BUCKET_NAME}" 2>/dev/null; then
  echo "Bucket already exists: ${BOOTSTRAP_BUCKET_NAME}"
else
  echo "Creating bucket: ${BOOTSTRAP_BUCKET_NAME}"

  if [ "${AWS_DEFAULT_REGION}" = "us-east-1" ]; then
    awslocal s3api create-bucket \
      --bucket "${BOOTSTRAP_BUCKET_NAME}"
  else
    awslocal s3api create-bucket \
      --bucket "${BOOTSTRAP_BUCKET_NAME}" \
      --create-bucket-configuration LocationConstraint="${AWS_DEFAULT_REGION}"
  fi

  echo "Bucket created: ${BOOTSTRAP_BUCKET_NAME}"
fi

echo "Current buckets:"
awslocal s3api list-buckets