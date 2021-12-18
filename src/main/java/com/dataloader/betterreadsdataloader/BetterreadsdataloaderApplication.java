package com.dataloader.betterreadsdataloader;

import com.dataloader.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsdataloaderApplication {

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsdataloaderApplication.class, args);
	}

	@Bean
	public CqlSessionBuilderCustomizer cqlSessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties){
		Path bundlePath = dataStaxAstraProperties.getSecureConnectBundle().toPath();
		return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundlePath);
	}
}
