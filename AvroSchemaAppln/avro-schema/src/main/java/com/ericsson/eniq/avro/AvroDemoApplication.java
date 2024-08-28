package com.ericsson.eniq.avro;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AvroDemoApplication {
	public static void main(String[] args) throws IOException {
		SpringApplication.run(AvroDemoApplication.class, args);
	}
}
