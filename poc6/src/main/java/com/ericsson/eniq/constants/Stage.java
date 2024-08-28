package com.ericsson.eniq.constants;

public enum Stage {
	
	TP_INSTALL_ORDERER("TPInstallOrderer"), PRE_INSTALL_CHECK("PreInstallCheck"),BACK_UP("BackUp"), META_INSTALLATION("MetaInstallation"),
	UPDATE_DATA_ITEM("UpdateDataItem"), ETL_SET_IMPORT("ETLSetImport"),HANDLE_BUSYHOUR_ACTIVATION("HandleBusyhourActivation"), 
	TECH_PACK_AND_TYPE_ACTIVATION("TechPackAndTypeActivation"), DWHM_STORAGE_TIME_ACTION("DWHMStorageTimeAction");

	    private final String value;

	    Stage(String value) {
	        this.value = value;
	    }
	   
	    public String getValue() {
	        return value;
	    }

}
