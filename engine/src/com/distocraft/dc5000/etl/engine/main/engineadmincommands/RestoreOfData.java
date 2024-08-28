package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public  class RestoreOfData extends Command {

	public  RestoreOfData(final String[] args) {
		super(args);
	}

	@Override
	void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
		
	}

	@Override
	String getUsageMessage() {
		return "triggerRestoreOfData";
	}

	@Override
	public void performCommand() throws Exception {
			EngineAdmin engineAdmin = createNewEngineAdmin();
			engineAdmin.triggerRestoreOfData();
	}

	@Override
	protected int getCorrectArgumentsLength() {
		return 1;
	}

	


	
}
