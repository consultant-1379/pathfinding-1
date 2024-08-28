package com.ericsson.eniq.common.setWizards;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.dwhrep.Dataformat;
import com.distocraft.dc5000.repository.dwhrep.DataformatFactory;
import com.distocraft.dc5000.repository.dwhrep.Defaulttags;
import com.distocraft.dc5000.repository.dwhrep.DefaulttagsFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacemeasurement;
import com.distocraft.dc5000.repository.dwhrep.InterfacemeasurementFactory;


/**
 * 
 * @author savinen Copyright Distocraft 2005
 * 
 * $id$
 */
public class InterfaceMigrationWizard {
	
	  protected RockFactory dwhrepRock;
	  protected String oldVersion = "";
	  protected String newVersion = "";
	  protected String interfaceName = "";

	  /**
	   * 
	   * constructor
	   * 
	   * @param version
	   * @param rock
	   * @param techPackID
	   * @param name
	   * @param adapterType
	   * @param adapterName
	   * @param connectionID
	   * @throws Exception
	   */
	  public InterfaceMigrationWizard(RockFactory dwhrepRock,String interfaceName, String oldVersion, String newVersion) throws Exception {


	    this.dwhrepRock = dwhrepRock;
	    this.oldVersion = oldVersion;
	    this.newVersion = newVersion;
	    this.interfaceName = interfaceName;
	  }
	    
	  protected String createInterface() throws Exception{
	
	  	StringBuffer msg = new StringBuffer();
	  	
	  	// old
	  	Map oldMap = new HashMap();
	  	//Query qOld = session.createQuery("from DataFormat where VERSIONID=?");
	  	//qOld.setString(0,this.oldVersion);
      
      Dataformat odf = new Dataformat(dwhrepRock);
      odf.setVersionid(this.oldVersion);
      DataformatFactory odff = new DataformatFactory(dwhrepRock,odf);
           
	  	Iterator oldDFIter = odff.get().iterator();
	  	while(oldDFIter.hasNext()){
	  		
	  		Dataformat odftmp = (Dataformat)oldDFIter.next();
		  	//Query q = session.createQuery("from DefaultTag where DATAFORMATID=?");
	      //q.setString(0,df.getDataformatid());
        
        Defaulttags odft = new Defaulttags(dwhrepRock);
        odft.setDataformatid(odftmp.getDataformatid());
        DefaulttagsFactory odftf = new DefaulttagsFactory(dwhrepRock,odft);
        
        
	      	Defaulttags deft = (Defaulttags)odftf.getElementAt(0);
	      	oldMap.put(odftmp.getDataformatid(),deft.getTagid());
	  	}
	  		
	  	if (oldMap.size()<=0){
	  		
	  		msg.append("No Dataformats found with version: "+this.oldVersion+" ");
	  		return msg.toString();
	  		
	  	}
	  	
	  	// new
	  	Map newMap = new HashMap();
	  	//Query qNew = session.createQuery("from DataFormat where VERSIONID=?");
	  	//qNew.setString(0,this.newVersion);
     
      Dataformat ndf = new Dataformat(dwhrepRock);
      ndf.setVersionid(this.newVersion);
      DataformatFactory ndff = new DataformatFactory(dwhrepRock,ndf);

      
	  	Iterator newDFIter = ndff.get().iterator();
	  	while(newDFIter.hasNext()){
	  		
	  		Dataformat df = (Dataformat)newDFIter.next();
		  	//Query q = session.createQuery("from DefaultTag where DATAFORMATID=?");
        //q.setString(0,df.getDataformatid());
       
        Defaulttags ndft = new Defaulttags(dwhrepRock);
        ndft.setDataformatid(df.getDataformatid());
        DefaulttagsFactory ndftf = new DefaulttagsFactory(dwhrepRock,ndft);
      
	      Defaulttags deft = (Defaulttags)ndftf.getElementAt(0);
	      newMap.put(deft.getTagid(),df);	      	
	  	}
	  	
	  	if (newMap.size()<=0){
	  		
	  		msg.append("No Dataformats found with version: "+this.newVersion+" ");
	  		return msg.toString();
	  		
	  	}
	  	 
	  	//Query q = session.createQuery("from InterfaceMeasurement where INTERFACENAME=?");
	  	//q.setString(0,this.interfaceName);
	  	//Iterator imIter = q.list().iterator();
      
      Interfacemeasurement im = new Interfacemeasurement(dwhrepRock);
      im.setInterfacename(this.interfaceName);
      InterfacemeasurementFactory imf = new InterfacemeasurementFactory(dwhrepRock,im);
      Iterator imIter = imf.get().iterator();
            
	  	while(imIter.hasNext()){
	  		Interfacemeasurement aim = (Interfacemeasurement)imIter.next();
	  		String dataformtaID = aim.getDataformatid();
        	  		
	  		if (oldMap.containsKey(dataformtaID)){
	  			
	  			// get new dataformatID 
	  			String tagID = (String)oldMap.get(dataformtaID);
	  			
          if (newMap.containsKey(tagID)){
            
              Dataformat df = (Dataformat)newMap.get(tagID);
              im.setDataformatid(df.getDataformatid());
              im.updateDB();

              msg.append("\n Changed "+dataformtaID+" ->  "+df.getDataformatid()+" in Version: "+this.interfaceName);
                            
              
          } else {
            
            // new version does not contain a dataformat anymore so it is deleted..
              Dataformat df = (Dataformat)newMap.get(tagID);
              
              im.deleteDB();
                         
              msg.append("\n Removed "+df.getDataformatid()+" in Version: "+this.interfaceName);
          }

	  				  			
	  		} else {
	  		  	msg.append("\n ERROR: Found '"+dataformtaID+"' in Version '"+this.interfaceName+"' when it should have been "+this.oldVersion);
	  		}
	  	}	  	
	  	
	  	
	  	return msg.toString();
	  	
	  }	  	

	  
	  
	  public String migrate() throws Exception {
	    
	    return createInterface();	
	  }

}
