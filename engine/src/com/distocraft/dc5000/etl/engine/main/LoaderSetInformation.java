package com.distocraft.dc5000.etl.engine.main;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;


public class LoaderSetInformation {
	
	private static HashMap<String,ArrayList<File>> interfaceToLoaderFileMap=new HashMap<String,ArrayList<File>>();;
	
	public LoaderSetInformation()
	{
		
	}

	public static HashMap<String,ArrayList<File>> getInterfaceToLoaderFileMap() {
		return interfaceToLoaderFileMap;
	}

	public static void setInterfaceToLoaderFileMap(HashMap<String, ArrayList<File>> interfaceToLoaderFileMap) {
		LoaderSetInformation.interfaceToLoaderFileMap = interfaceToLoaderFileMap;
	}
	

	
	
	
}
