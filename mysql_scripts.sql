-- Test merge table data ~
Drop Table hdfs_dir_meta_bak_20200427;

-- 复制表 Statement violates GTID consistency, https://stackoverflow.com/questions/40724046/mysql-gtid-consistency-violation
CREATE TABLE hdfs_dir_meta_bak_20200427 SELECT * FROM
    hdfs_dir_meta;
    
-- Solution
CREATE TABLE hdfs_dir_meta_bak_20200427 LIKE hdfs_dir_meta;
insert hdfs_dir_meta_bak_20200427 select * from hdfs_dir_meta;

-- merge tables, hdfs_dir_meta.data_name solve "Error Code: 1052. Column 'data_name' in field list is ambiguous	0.0030 sec"
insert hdfs_dir_meta
	select * from hdfs_dir_meta_to_merge
	on duplicate key update 
		data_name = if(isnull(values(data_name)) = 1 or length(trim(values(data_name))) = 0, hdfs_dir_meta.data_name, values(data_name) ), 
		security_level = if(isnull(values(security_level)) = 1 or length(trim(values(security_level))) = 0, hdfs_dir_meta.security_level, values(security_level)) , 
		upload_side = if(isnull(values(upload_side)) = 1 or length(trim(values(upload_side))) = 0, hdfs_dir_meta.upload_side, values(upload_side)) , 
		project = if(isnull(values(project)) = 1 or length(trim(values(project))) = 0, hdfs_dir_meta.project, values(project)) , 
		data_source = if(isnull(values(data_source)) = 1 or length(trim(values(data_source))) = 0, hdfs_dir_meta.data_source, values(data_source)) , 
		data_source_industry = if(isnull(values(data_source_industry)) or length(trim(values(data_source_industry))) = 0, hdfs_dir_meta.data_source_industry, values(data_source_industry)) , 
		data_spec = if(isnull(values(data_spec)) or length(trim(values(data_spec))) = 0, hdfs_dir_meta.data_spec, values(data_spec)) , 
		data_use = if(isnull(values(data_use)) or length(trim(values(data_use))) = 0, hdfs_dir_meta.data_use, values(data_use)) , 
		remark = if(isnull(values(remark)) or length(trim(values(remark))) = 0, hdfs_dir_meta.remark, values(remark)) , 
		cluster = if(isnull(values(cluster)) or length(trim(values(cluster))) = 0, hdfs_dir_meta.cluster, values(cluster)) , 
		absolute_path = if(isnull(values(absolute_path)) or length(trim(values(absolute_path))) = 0, hdfs_dir_meta.absolute_path, values(absolute_path)) , 
		is_encrypt_zone = if(isnull(values(is_encrypt_zone)) or length(trim(values(is_encrypt_zone))) = 0, hdfs_dir_meta.is_encrypt_zone, values(is_encrypt_zone)) , 
		has_encrypted = if(isnull(values(has_encrypted)) or length(trim(values(has_encrypted))) = 0, hdfs_dir_meta.has_encrypted, values(has_encrypted)) , 
		actual_data_type = if(isnull(values(actual_data_type)) or length(trim(values(actual_data_type))) = 0, hdfs_dir_meta.actual_data_type, values(actual_data_type)) , 
		actual_data_sub_type = if(isnull(values(actual_data_sub_type)) or length(trim(values(actual_data_sub_type))) = 0, hdfs_dir_meta.actual_data_sub_type, values(actual_data_sub_type));
