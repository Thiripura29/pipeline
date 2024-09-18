def main(df_dict, config_object):
    loan_approval_df = df_dict['bronze_loan_approval_source']
    return {"silver_loan_processing_dq_checks": loan_approval_df}
