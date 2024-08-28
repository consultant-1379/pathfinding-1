package com.ericsson.eniq.avro.repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.ericsson.eniq.avro.config.DBConnection;
import com.ericsson.eniq.avro.entity.AvroMapping;
import com.ericsson.eniq.avro.entity.DataFormat;
import com.ericsson.eniq.avro.entity.DataItem;
import com.ericsson.eniq.avro.entity.DefaultTags;

@Repository
public class SchemaRepository {
	/*
	 * @Autowired
	 * @Qualifier("npJdbcTemp") 
	 * private NamedParameterJdbcTemplate npJdbcTemplate;
	 * @Autowired
	 * @Qualifier("jdbcTemp")
	 * private JdbcTemplate jdbcTemplate;
	 */
	@Autowired
	private DBConnection dbConnectionUtil;
	public List<DataFormat> getTypeIds(String techPack) {
		DataSource source = dbConnectionUtil.getDataSource();
		NamedParameterJdbcTemplate npJdbcTemplate = new NamedParameterJdbcTemplate(source);
		MapSqlParameterSource queryParams = new MapSqlParameterSource();
		queryParams.addValue("techPack", techPack);
		return npJdbcTemplate.query("SELECT DISTINCT TYPEID FROM DataFormat WHERE VERSIONID=:techPack",
				queryParams, 
				(rs, rowNum) -> new DataFormat(
						rs.getString("TYPEID")));
	}
	public List<DataItem> getDataItems(String dataFormatId) {
		MapSqlParameterSource queryParams = new MapSqlParameterSource();
		queryParams.addValue("dataFormatId", dataFormatId+":%");
		DataSource source = dbConnectionUtil.getDataSource();
		NamedParameterJdbcTemplate npJdbcTemplate = new NamedParameterJdbcTemplate(source);
		return npJdbcTemplate.query("SELECT DISTINCT DATANAME, DATATYPE FROM DataItem where DATAFORMATID LIKE :dataFormatId",
				queryParams, (rs, rowNum) -> new DataItem(
						rs.getString("DATANAME"),
						rs.getString("DATATYPE")));
	}
	
	public List<DefaultTags> getDefaultTags(String dataFormatId) {
		MapSqlParameterSource queryParams = new MapSqlParameterSource();
		queryParams.addValue("dataFormatId", dataFormatId+":%");
		DataSource source = dbConnectionUtil.getDataSource();
		NamedParameterJdbcTemplate npJdbcTemplate = new NamedParameterJdbcTemplate(source);
		return npJdbcTemplate.query("SELECT COUMAP.DATAFORMATID,MOMAP.TAGID,COUMAP.DATANAME,COUMAP.DATAID,COUMAP.DATATYPE, COUMAP.DATASIZE "
				+ " FROM DefaultTags MOMAP, DataItem COUMAP WHERE MOMAP.DATAFORMATID=COUMAP.DATAFORMATID AND COUMAP.DATAFORMATID LIKE :dataFormatId",
				queryParams,
				(rs, rowNum) -> new DefaultTags(
						rs.getString("DATAFORMATID"),
						rs.getString("TAGID"),
						rs.getString("DATANAME"),
						rs.getString("DATAID"),
						rs.getString("DATATYPE"),
						rs.getInt("DATASIZE")
						));
	}
	@Transactional
	public void deleteAvroMappings(String techPack,String iqTable)
	{
		MapSqlParameterSource queryParams = new MapSqlParameterSource();
		queryParams.addValue("techPack", techPack);
		queryParams.addValue("iqTable", iqTable);
		DataSource source = dbConnectionUtil.getDataSource();
		NamedParameterJdbcTemplate npJdbcTemplate = new NamedParameterJdbcTemplate(source);
		 npJdbcTemplate.update("DELETE FROM AvroMapping WHERE VERSIONID=:techPack AND IQTABLE=:iqTable", 
				 queryParams);
		
	}
	public List<AvroMapping> getAvroMappings(String techPack) {
		MapSqlParameterSource queryParams = new MapSqlParameterSource();
		queryParams.addValue("techPack", techPack);
		DataSource source = dbConnectionUtil.getDataSource();
		NamedParameterJdbcTemplate npJdbcTemplate = new NamedParameterJdbcTemplate(source);
		return npJdbcTemplate.query("SELECT * FROM AvroMapping WHERE VERSIONID=:techPack",
				queryParams,
				(rs, rowNum) -> new AvroMapping(
						rs.getString("VERSIONID"),
						rs.getString("MOCLASS"),
						rs.getString("AVRONAME"),
						rs.getString("IQTABLE"), 
						rs.getString("DBCOLUMN"),
						rs.getString("AVROCOLUMN"), 
						rs.getString("DATAFORMAT"),
						rs.getString("FILEDATATYPE"),
						rs.getString("AVRODATATYPE"),
						rs.getInt("DATASIZE")));

	}
	@Transactional
	 public int[] batchInsertAvroMappings(List<AvroMapping> avroMappings) {
			DataSource source = dbConnectionUtil.getDataSource();
			JdbcTemplate jdbcTemplate = new JdbcTemplate(source);
	        return jdbcTemplate.batchUpdate(
	            "INSERT INTO AvroMapping (VERSIONID, MOCLASS,AVRONAME,IQTABLE,DBCOLUMN,AVROCOLUMN,DATAFORMAT,FILEDATATYPE,AVRODATATYPE,DATASIZE) "
	                                    + " VALUES(?,?,?,?,?,?,?,?,?,?)",
	            new BatchPreparedStatementSetter() {
	                public void setValues(PreparedStatement ps, int i) throws SQLException {
	                    ps.setString(1, avroMappings.get(i).getVersionId());
	                    ps.setString(2, avroMappings.get(i).getMoClass());
	                    ps.setString(3, avroMappings.get(i).getAvroName());
	                    ps.setString(4, avroMappings.get(i).getIqTable());
	                    ps.setString(5, avroMappings.get(i).getDbColumn());
	                    ps.setString(6, avroMappings.get(i).getAvroColumn());
	                    ps.setString(7, avroMappings.get(i).getDataFormat());
	                    ps.setString(8, avroMappings.get(i).getFieldDataType());
	                    ps.setString(9, avroMappings.get(i).getAvroDataType());
	                    ps.setInt(10, avroMappings.get(i).getDataSize());
	                }

	                public int getBatchSize() {
	                    return avroMappings.size();
	                }

	            });
	    }
}
