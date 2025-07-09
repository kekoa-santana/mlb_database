# MLB Data Pipeline

This repository contains an AWS Lambda based pipeline for fetching and storing MLB Statcast data in PostgreSQL. The project includes helper utilities, unit tests, and a Step Functions workflow for historical backfill.

## Environment Variables

The Lambda functions and unit tests require access to a PostgreSQL database. Create a `.env` file in `config/` (or export the variables in your shell) with the following settings:

```bash
RDS_HOST=<hostname>
RDS_DATABASE=<database_name>
RDS_USERNAME=<username>
RDS_PASSWORD=<password>
# Optional, defaults to 5432
RDS_PORT=5432
```

A sample file is provided at `config/.env.example`.

## Running Tests Locally

Install the dependencies and run `pytest`:

```bash
pip install -r requirements.txt
pytest -v tests
```

The tests expect the environment variables above to be available (from `config/.env` or your shell) so that database connections can be established.

## Building the Docker Image

A `Dockerfile` is provided for deploying the Lambda as a container image. Build it with:

```bash
docker build -t mlb-data-pipeline .
```

Push the image to Amazon ECR and create or update your Lambda function to use this image.

## Deploying the Step Functions Workflow

The file `mlb_data_pipeline_stepfunction.json` describes the Step Functions state machine used for historical backfills. Deploy it with the AWS CLI:

```bash
aws stepfunctions create-state-machine \
  --name mlb-data-pipeline \
  --definition file://mlb_data_pipeline_stepfunction.json \
  --role-arn <execution-role-arn>
```

Update the Lambda ARN in the JSON if your function name differs. Once created, you can trigger the workflow from the AWS console or via `start-execution`.
