package com.ericsson.eniq.InstallerApplication;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
@SpringBootApplication(scanBasePackages= {"com.ericsson.eniq.Controller","com.ericsson.eniq.Services","com.ericsson.eniq.InstallerApplication"})
//@EnableCaching
public class InstallerApplication{

	public static void main(String[] args) {
		SpringApplication.run(InstallerApplication.class, args);
	}


}
