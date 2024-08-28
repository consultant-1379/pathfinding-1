package com.ericsson.eniq.InstallerApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ericsson.eniq.bean.GetLicenseResponse;

@SpringBootApplication(scanBasePackages = { "com.ericsson.eniq.Controller", "com.ericsson.eniq.bean",
		"com.ericsson.eniq.utils", "com.ericsson.eniq.config", "com.ericsson.eniq.service",
		"com.ericsson.eniq.Services", "com.ericsson.eniq.InstallerApplication" })
//@EnableCaching
public class InstallerApplication implements CommandLineRunner {

	@Autowired
	private GetLicenseResponse getLicenseResponse;

	public static void main(String[] args) {
		SpringApplication.run(InstallerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println(getLicenseResponse);
	}

}
