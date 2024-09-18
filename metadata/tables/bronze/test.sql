DROP TABLE IF EXISTS `{{ database }}`.`customer_features`;
CREATE TABLE `{{ database }}`.customer_features (
  customer_id int NOT NULL,
  feat1 long,
  feat2 varchar(100),
  CONSTRAINT customer_features_pk PRIMARY KEY (customer_id)
)
USING DELTA
LOCATION '{{ feature_store_schema_path }}/customer_features'