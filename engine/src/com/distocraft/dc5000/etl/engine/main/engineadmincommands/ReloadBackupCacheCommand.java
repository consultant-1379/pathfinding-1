/**
 * 
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author xthobob
 *
 */
public class ReloadBackupCacheCommand extends Command {
	
	public ReloadBackupCacheCommand(final String[] args) {
		super(args);
	}

	@Override
	void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
		// Nothing to do
		
	}

	@Override
	String getUsageMessage() {
		return "reloadBackupCache";
	}

	@Override
	public void performCommand() throws Exception {
		final EngineAdmin admin = new EngineAdmin();
		admin.reloadBackupConfigCache();
		
	}

	@Override
	protected int getCorrectArgumentsLength() {
		// TODO Auto-generated method stub
		return 1;
	}

}
