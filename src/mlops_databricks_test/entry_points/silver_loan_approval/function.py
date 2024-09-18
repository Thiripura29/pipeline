def main(df_dict, config_object):
    dq_loan_approval_source = df_dict['intermediate_silver_loan_approval_source']
    good_records = dq_loan_approval_source.where("dq_validations.run_row_success == true").drop('dq_validations')
    bad_records = dq_loan_approval_source.where("dq_validations.run_row_success == false")
    return {"write_silver_loan_approval_bad_records": bad_records, "write_silver_loan_approval_good_records": good_records}
