package com.ericsson.oss.eniq.techpack.utility;


import java.util.ArrayList;
import java.util.List;

import com.ericsson.oss.eniq.techpack.model.AvroModel;
import com.ericsson.oss.eniq.techpack.model.FieldsModel;

public class TechpackStub {

	
	public AvroModel testReadAvroModel() throws Exception {
		AvroModel avroModel = new AvroModel();
		List<FieldsModel> fields = new ArrayList<>();
		FieldsModel model = new FieldsModel();
		avroModel.setType("record");
		avroModel.setNamespace("com.ericsson.eniq.data");
		avroModel.setName("DC_E_ERBS_ADMISSIONCONTROL_V_DAY");
		model.setName("ERBS");
		model.setType("String");
		avroModel.setFields(fields);
		avroModel.add(fields);
		return avroModel;

	}
}
