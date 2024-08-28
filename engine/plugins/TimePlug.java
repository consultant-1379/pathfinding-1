import java.util.logging.Logger;

import com.distocraft.dc5000.etl.engine.plugin.PluginClass;
import com.distocraft.dc5000.etl.engine.plugin.PluginException;



 /**
 *
 * Class for using FTP-connections
 *
 */

public class TimePlug implements PluginClass{
    

 	 private Logger log = Logger.getLogger("Test"); 
 	 private long seconds;
 	 private String message="";
 	 
 	 
/** Constructor
*   
*   @param  String userId           The user account name, required
*
*/

  public TimePlug (String instanceName, String time) throws PluginException  
  {
        
  	
	 if (instanceName == null) {
		throw new PluginException("instance.name required");
	 }

	 if (time == null) {
		throw new PluginException("Time required");
	 }

	 this.message = instanceName;
	 this.seconds = Long.parseLong(time);
	 	
	 
  }
	              
	 
	 
     
  /**
  * This  method executes the system command and
  * transfers a file via FTP.
  * 
  */

  public void commit() throws PluginException
  {

	  	for (int i = 0; i < this.seconds; i++)
		  	{
	  			try
				{
			  		Thread.sleep(1000);
			  		log.finest(this.message+" "+i);
	  				
				}
	  			catch (Exception e)
				{
	  				
				}
		  	}

  	
  }
    

   /**
   @return names of constructor parameters and a brief comment of each one
   *
   */
   public static String getConstructorParameterInfo(){  
    return
    "instance.name, required \n"+
    "time (seconds), required \n";
    }
   
       
   
}



