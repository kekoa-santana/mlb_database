
export PGHOST="mlb-pipeline-db.cfguwseu2qrk.us-east-2.rds.amazonaws.com"
export PGHOSTADDR="127.0.0.1"     
export PGPORT="5433"              
export PGDATABASE="mlb_statcast_features"
export PGUSER="app_user"
export PGSSLMODE="verify-full"   
export REGION="us-east-2"

export PGPASSWORD=$(aws rds generate-db-auth-token \
  --hostname "$PGHOST" --port 5432 --region "$REGION" --username "$PGUSER")

# 3) Run your SQL file
psql -v ON_ERROR_STOP=1 -f duplicate_checks.sql


-- aws ssm start-session \
--   --target i-02c24db9d6d4c109e \
--   --document-name AWS-StartPortForwardingSessionToRemoteHost \
--   --parameters host="mlb-pipeline-db.cfguwseu2qrk.us-east-2.rds.amazonaws.com",portNumber="5432",localPortNumber="5433"
