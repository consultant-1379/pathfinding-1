package com.ericsson.eniq.bean;

public class OperationalStatus {

	private OperationalMode operationalMode; //  
	private Integer autonomousModeDuration;

	public OperationalMode getOperationalMode() {
		return operationalMode;
	}

	public void setOperationalMode(OperationalMode operationalMode) {
		this.operationalMode = operationalMode;
	}

	public Integer getAutonomousModeDuration() {
		return autonomousModeDuration;
	}

	public void setAutonomousModeDuration(Integer autonomousModeDuration) {
		this.autonomousModeDuration = autonomousModeDuration;
	}

	@Override
	public String toString() {
		return "OperationalStatus [operationalMode=" + operationalMode + ", autonomousModeDuration="
				+ autonomousModeDuration + "]";
	}

	
}
