package com.broadcom.springconsulting.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.net.MalformedURLException;

@SpringBootApplication
@EnableBatchProcessing
public class BatchDemoApplication {

	public static void main( String[] args ) {

		System.exit(
				SpringApplication.exit(
						SpringApplication.run( BatchDemoApplication.class, args )
				)
		);

	}

	@Bean
	@StepScope
	public Resource resource( @Value( "#{jobParameters['fileName']}" ) String fileName ) throws MalformedURLException {

		return new UrlResource( fileName );
	}

	@Bean
	public FlatFileItemReader<Person> itemReader( Resource resource )  {

		return new FlatFileItemReaderBuilder<Person>()
				.name( "personItemReader" )
				.resource( resource )
				.delimited()
				.names( "firstName", "lastName" )
				.targetType( Person.class )
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Person> itemWriter( DataSource dataSource ) {

		return new JdbcBatchItemWriterBuilder<Person>()
				.dataSource( dataSource )
				.sql( "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)" )
				.beanMapped()
				.build();
	}

	@Bean
	Step step1(
			JobRepository jobRepository, PlatformTransactionManager transactionManager,
			ItemReader<Person> itemReader, ItemWriter<Person> itemWriter
	) {

		return new StepBuilder( "step1", jobRepository )
				.<Person, Person> chunk( 3, transactionManager )
				.reader( itemReader )
				.writer( itemWriter )
				.build();
	}

	@Bean
	Job job( JobRepository jobRepository, Step step1 ) {

		return new JobBuilder("job", jobRepository)
                .start(step1)
				.build();
	}

	public record Person( String firstName, String lastName ) { }

}
