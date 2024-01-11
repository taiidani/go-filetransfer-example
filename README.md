# Go Filetransfer Example

This repository contains a proof of concept for a CLI script that uploads a directory of files to S3 with support for concurrency and progress tracking.

To run it, execute `go run main.go <Directory To Upload> <Bucket Name> [Bucket Prefix]`

You will need at minimum the following environment variables set:
* `AWS_ACCESS_KEY_ID` the access key ID for an AWS IAM user with S3 write access.
* `AWS_SECRET_ACCESS_KEY` the secret access key for an AWS IAM user with S3 write access.
* `AWS_DEFAULT_REGION` the region that your S3 bucket resides in, such as `us-west-2`.
