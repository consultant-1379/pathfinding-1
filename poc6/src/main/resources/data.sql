  INSERT INTO tech_pack_activation (id, tp_name, tp_version, tp_status) VALUES 
  (101, 'techpack1','1.12', 'de-active'),
  (102, 'techpack2','1.86', 'de-active'),
  (103, 'techpack3','1.65', 'de-active');
  
  
  INSERT INTO feature_activation (id, license_no, feature_description, techpacks, status) VALUES 
 (11, 'feature1', 'UC0012', 'active' , 'pack1'),
  (12, 'feature2', 'UC0013', 'de-active', 'pack2'),
  (13, 'feature3', 'UC0014', 'active', 'pack3');
  
  