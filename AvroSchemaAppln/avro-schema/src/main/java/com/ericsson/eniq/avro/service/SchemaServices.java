package com.ericsson.eniq.avro.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.avro.entity.AvroMapping;
import com.ericsson.eniq.avro.entity.DataFormat;
import com.ericsson.eniq.avro.entity.DataItem;
import com.ericsson.eniq.avro.entity.DefaultTags;
import com.ericsson.eniq.avro.feign.SchemaRegistoryFeign;
import com.ericsson.eniq.avro.model.AvrSchema;
import com.ericsson.eniq.avro.model.AvrSchemaBody;
import com.ericsson.eniq.avro.model.AvrSubject;
import com.ericsson.eniq.avro.model.Field;
import com.ericsson.eniq.avro.model.LogicalType;
import com.ericsson.eniq.avro.repository.SchemaRepository;
import com.ericsson.eniq.avro.util.CommonUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class SchemaServices {
	private static final Logger LOG = LoggerFactory.getLogger(SchemaServices.class);
	@Autowired
	SchemaRegistoryFeign schemaRegistoryFeign;
	@Autowired
	SchemaRepository schemaRepository;

	public List<String> createAvroSchemas(String techPack) {
		List<String> avrSchemas = null;
		List<DataFormat> dataFormats = null;
		try {
			avrSchemas = new ArrayList<String>();
			dataFormats = schemaRepository.getTypeIds(techPack);
			List<DefaultTags> defaultTags = null;
			List<DataItem> dataItems = null;
			AvrSchemaBody avrSchemaBody = null;
			for (DataFormat dataFormat : dataFormats) {
				avrSchemaBody = new AvrSchemaBody();
				String versionId = techPack;
				String iqTable = dataFormat.getTypeId().replace(techPack + ":", "").trim();
				avrSchemaBody.setNamespace("com.ericsson.eniq.avro");
				avrSchemaBody.setType("record");
				avrSchemaBody.setName(iqTable);
				Field field = null;
				List<Field> fields = new ArrayList<Field>();
				dataItems = schemaRepository.getDataItems(dataFormat.getTypeId());
				for (DataItem dataItem : dataItems) {
					field = new Field();
					field.setName(dataItem.getDataName());
					String lgcType[] = CommonUtil.getAvroType(dataItem.getDataType()).split(",");
					field.setType(lgcType[0]);
					if (lgcType.length > 1) {
						LogicalType logicalType = new LogicalType();
						logicalType.setType(lgcType[0]);
						logicalType.setLogicalType(lgcType[1]);
						field.setType(logicalType);
					}
					fields.add(field);
				}
				avrSchemaBody.setFields(fields);
				Gson gson = new GsonBuilder().serializeNulls().create();
				String schemaString = gson.toJson(avrSchemaBody, AvrSchemaBody.class);
				/*
				 * LOG.info("Schema String : "+schemaString); Schema.Parser parser = new
				 * Schema.Parser(); System.out.println(parser.parse(schemaString));
				 */
				AvrSchema avrSchema = new AvrSchema();
				avrSchema.setSchema(schemaString);
				String subjectName = techPack + "-" + iqTable;
				String json = CommonUtil.objectToJson(avrSchema);
				LOG.info("Register new schema for subject name,data {}", subjectName, json);
				ResponseEntity<String> response = schemaRegistoryFeign.addSchema(subjectName, json);
				LOG.info("Response for registor schema {}", response.getBody());
				if (String.valueOf(response.getStatusCodeValue()).startsWith("2")) {
					avrSchemas.add(response.getBody());
					defaultTags = schemaRepository.getDefaultTags(dataFormat.getTypeId());
					AvroMapping avroMapping = null;
					List<AvroMapping> avroMappings = new ArrayList<AvroMapping>();
					for (DefaultTags defaultTag : defaultTags) {
						avroMapping = new AvroMapping();
						avroMapping.setVersionId(versionId);
						avroMapping.setIqTable(iqTable);
						avroMapping.setAvroName(iqTable);
						avroMapping.setDbColumn(defaultTag.getDataName());
						avroMapping.setAvroColumn(defaultTag.getDataName());
						avroMapping.setFieldDataType(defaultTag.getDataType());
						avroMapping.setAvroDataType(CommonUtil.getAvroType(defaultTag.getDataType()));
						avroMapping.setDataFormat(defaultTag.getDataId());
						avroMapping.setMoClass(defaultTag.getTagId());
						avroMapping.setDataSize(defaultTag.getDataSize());
						avroMappings.add(avroMapping);
					}

					LOG.info("Delete existing records from AvroMapping", techPack, iqTable);
					schemaRepository.deleteAvroMappings(techPack, iqTable);
					LOG.info("Successfully deleted existing records from AvroMapping", techPack, iqTable);
					LOG.info("Insert records into AvroMapping", techPack, iqTable);
					schemaRepository.batchInsertAvroMappings(avroMappings);
					LOG.info("Successfully inserted records into AvroMapping", techPack, iqTable);
				}
			}

		} catch (Exception e) {
			LOG.error("Something went wrong!", e);
		}
		return avrSchemas;

	}

	public List<String> getAllSubjects() {
		List<String> res = schemaRegistoryFeign.getAllSubjects();
		return res;

	}

	public AvrSubject getSchema(String subjectName) {
		return schemaRegistoryFeign.getLatestSchemas(subjectName);
	}

	public String deleteSubject(String subjectName) {
		try {
			schemaRegistoryFeign.deleteSchema(subjectName);
		} catch (Exception e) {
			LOG.error("Something went wrong! ", e);
			return "Something went wrong";
		}
		return "Successfully deleted";
	}

	public String deleteAllSubjects() {
		try {
			List<String> subjects = schemaRegistoryFeign.getAllSubjects();
			for (String subjectName : subjects) {
				schemaRegistoryFeign.deleteSchema(subjectName);
			}
		} catch (Exception e) {
			LOG.error("Something went wrong! ", e);
			return "Something went wrong";
		}
		return "Successfully deleted";
	}

	public List<String> createAvroSchemasApproach1(String techPack) {
		List<String> avrSchemas = null;
		try {
			avrSchemas = new ArrayList<String>();
			AvrSchemaBody avrSchemaBody = null;
			List<AvroMapping> avroMappings = schemaRepository.getAvroMappings(techPack);
			Map<String, List<AvroMapping>> map = avroMappings.stream()
					.collect(Collectors.groupingBy(e -> e.getIqTable()));
			for (String iqTable : map.keySet()) {
				avrSchemaBody = new AvrSchemaBody();
				avrSchemaBody.setNamespace("com.ericsson.eniq.avro");
				avrSchemaBody.setType("record");
				avrSchemaBody.setName(iqTable);
				Field field = null;
				List<Field> fields = new ArrayList<Field>();
				for (AvroMapping dataItem : map.get(iqTable)) {
					field = new Field();
					field.setName(dataItem.getAvroColumn());
					String lgcType[] = CommonUtil.getAvroType(dataItem.getFieldDataType()).split(",");
					field.setType(lgcType[0]);
					if (lgcType.length > 1) {
						LogicalType logicalType = new LogicalType();
						logicalType.setType(lgcType[0]);
						logicalType.setLogicalType(lgcType[1]);
						field.setType(logicalType);
					}
					fields.add(field);
				}
				avrSchemaBody.setFields(fields);
				Gson gson = new GsonBuilder().serializeNulls().create();
				String schemaString = gson.toJson(avrSchemaBody, AvrSchemaBody.class);

				AvrSchema avrSchema = new AvrSchema();
				avrSchema.setSchema(schemaString);
				String subjectName = techPack + "-" + iqTable;
				String json = CommonUtil.objectToJson(avrSchema);
				LOG.info("Register new schema for subject name,data {}", subjectName, json);
				ResponseEntity<String> response = schemaRegistoryFeign.addSchema(subjectName, json);
				LOG.info("Response for registor schema {}", response.getBody());
				avrSchemas.add(response.getBody());
			}

		} catch (Exception e) {
			LOG.error("Something went wrong!", e);
		}
		return avrSchemas;

	}

}
