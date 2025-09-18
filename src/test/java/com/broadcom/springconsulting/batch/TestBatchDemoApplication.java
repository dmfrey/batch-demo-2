package com.broadcom.springconsulting.batch;

import org.springframework.boot.SpringApplication;

public class TestBatchDemoApplication {

	public static void main(String[] args) {

		SpringApplication.from( BatchDemoApplication::main )
				.with( TestcontainersConfiguration.class )
				.withAdditionalProfiles( "test" )
				.run( args );

	}

}
