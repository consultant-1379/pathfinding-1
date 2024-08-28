package com.ericsson.oss.eniq.techpack.model;

import lombok.Data;

@Data
public class Versioning{

    private String versionid;
    private String description;
    private Long status;
    private String techpack_name;
    private String techpack_version;
    private String techpack_type;
    private String product_number;
    private String lockedby;
    private String lockdate;
    private String basedefinition;
    private String baseversion;
    private String installdescription;
    private String eniq_level;
    private String licensename;

}
