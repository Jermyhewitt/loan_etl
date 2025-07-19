#!/bin/bash

SCHEMA="etl_source"
DATABASE="etl_source"
OUTPUT_DIR="models/bronze/sources"

mkdir -p "$OUTPUT_DIR"
echo "✅ Output directory created at $OUTPUT_DIR"

# List of tables to generate sources for
tables=(deposit_confirm interest_rates creditor loan_likes roles user_deposit loan_transactions user_employer config loan_address loan_repayment investor user_payment score_card refinance user fees loan_buyer investment_owner user_account auto_investor user_loan investment withdraw loan_extra_payment loan_late_payment images user_address score_weights loan_images debtor loan_resale system_charges user_bank employer loan_account user_loan_view credit_score address loan_documents loan_instant_reborrow loan)

echo "📦 Tables found: ${tables[@]}"

# Loop over each table and generate source yaml
for table in "${tables[@]}"; do
  echo "🛠️ Generating source for table: $table"

pipenv run dbt --quiet run-operation generate_source \
  --args "{\"table_names\": [\"$table\"],\"generate_columns\": true, \"schema_name\":$SCHEMA}" \
  > "$OUTPUT_DIR/br_${table}.yml"

  # Proper variable expansion inside JSON string + disable colo
done
