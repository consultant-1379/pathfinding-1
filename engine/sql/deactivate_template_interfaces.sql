UPDATE etlrep.meta_collection_sets
SET enabled_flag = 'N' 
WHERE type = 'Interface' AND collection_set_name NOT LIKE '%-%' AND enabled_flag = 'Y';