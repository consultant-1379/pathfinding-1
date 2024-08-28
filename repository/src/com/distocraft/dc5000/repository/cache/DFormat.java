package com.distocraft.dc5000.repository.cache;

import java.util.Iterator;
import java.util.List;

public class DFormat {
  
  private final String interfaceName;
  private final String tagID;
  private final String dataFormatID;
  private final String folderName;
  private final String transformerID;
  
  private List<DItem> ditems = null;
  
  public DFormat(final String ifname,final  String tid, final String dfid, final String fname, final String trID) {
    interfaceName = ifname;
    tagID = tid;
    dataFormatID = dfid;
    folderName = fname;
    transformerID = trID;
  }

  public String getDataFormatID() {
    return dataFormatID;
  }

  public List<DItem> getDitems() {
    return ditems;
  }

  public String getFolderName() {
    return folderName;
  }

  public String getTransformerID() {
    return transformerID;
  }

  
  public String getInterfaceName() {
    return interfaceName;
  }

  public String getTagID() {
    return tagID;
  }
  
  public int getDItemCount() {
    return ditems.size();
  }
  
  public Iterator<DItem> getDItems() {
    return ditems.iterator();
  }
  
  public void setItems(List<DItem> list) {
    ditems = list;
  }
  
  @Override
public String toString() {
	return "DFormat [interfaceName=" + interfaceName + ", tagID=" + tagID + ", dataFormatID=" + dataFormatID
			+ ", folderName=" + folderName + ", transformerID=" + transformerID + ", ditems=" + ditems + "]";
}

public List<DItem> getItems() {
	  return ditems;
  }
  
}
