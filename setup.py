
from setuptools import setup, find_namespace_packages

requirements = []
with open('requirements.txt', 'r') as file:
    for line in file:
        line = line.strip()
        requirements.append(line.strip())

# Minimal example for versioning purposes, not ready yet.
setup(
    name="mlops_databricks_test",
    version="0.1",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[requirements],
    python_requires=">=3.7",
    include_package_data=True,
    entry_points={
        "console_scripts": ['staging_bronze_loan_approval=mlops_databricks_test.entry_points.staging_bronze_loan_approval.entry_point_staging_bronze_loan_approval:main',
							'bronze_silver_dq_loan_approval=mlops_databricks_test.entry_points.bronze_silver_dq_loan_approval.entry_point_bronze_silver_dq_loan_approval:main',
							'silver_loan_approval=mlops_databricks_test.entry_points.silver_loan_approval.entry_point_silver_loan_approval:main']
    }
)
    