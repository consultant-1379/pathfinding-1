package com.ericsson.eniq.flssymlink.fls;


import java.rmi.Naming;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.ericsson.eniq.enminterworking.EnmInterCommonUtils;
import com.ericsson.eniq.enminterworking.EnmInterworking;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;

public class FlsStopMain {

	public static void main(String[] args){
		Logger log = Logger.getLogger("symboliclinkcreator.fls");
		try{
		
		IEnmInterworkingRMI multiEs = (IEnmInterworkingRMI) Naming
				.lookup(RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterCommonUtils.getEngineIP()));
		multiEs.shutDownMain();
		
		}
		catch(Exception e){
			
		}
	

	}

}
