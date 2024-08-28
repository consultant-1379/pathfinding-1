DROP TABLE IF EXISTS feature_activation;

CREATE TABLE feature_activation (
  id INT AUTO_INCREMENT  PRIMARY KEY,
  license_no VARCHAR(250) NOT NULL,
  feature_description VARCHAR(250) NOT NULL,
  techpacks VARCHAR(250),
  status VARCHAR(250) NOT NULL
);

DROP TABLE IF EXISTS tech_pack_activation;

CREATE TABLE tech_pack_activation (
 id INT AUTO_INCREMENT  PRIMARY KEY,
 tp_name VARCHAR(250) NOT NULL,
 tp_version VARCHAR(250) NOT NULL,
 tp_status VARCHAR(250) NOT NULL
 );



