from pyspark.sql import functions as F


def main(df_dict, config_object):
    loan_approval_df = df_dict['staging_bronze_loan_approval_source']
    # Get a list of all column names
    columns = loan_approval_df.columns

    # Concatenate all columns into a single string column and then hash it
    loan_approval_df = loan_approval_df.withColumn("source_record_id", F.hash(F.concat_ws("||", *columns)))

    # Additional columns as before
    loan_approval_df = loan_approval_df.withColumn("ingestion_timestamp", F.current_timestamp())

    return {"bronze_loan_approval_delta_sink": loan_approval_df}
