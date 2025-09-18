package com.broadcom.springconsulting.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.integration.dsl.FunctionFlowBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;
import java.util.function.Function;

@SpringBootApplication
public class BatchDemoApplication {

	public static void main( String[] args ) {

		System.exit(
				SpringApplication.exit(
						SpringApplication.run( BatchDemoApplication.class, args )
				)
		);

	}

	@Bean
	public TaskExecutor batchTaskExecutor() {

		return new VirtualThreadTaskExecutor("spring-batch-virtual-");
	}

	@Bean
	public JobLaunchingGateway jobLaunchingGateway( JobRepository jobRepository ) {

		var jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository( jobRepository );
		jobLauncher.setTaskExecutor( new SyncTaskExecutor() );

		return new JobLaunchingGateway( jobLauncher );
	}

	@Bean
	Function<File, JobLaunchRequest> jobLaunchRequestFunction( Job job ) {

		return f -> {

			var jobParametersBuilder = new JobParametersBuilder();
			jobParametersBuilder.addString( "input.file.name", f.getAbsolutePath() );

			return new JobLaunchRequest( job, jobParametersBuilder.toJobParameters() );
		};
	}

	@Bean
	public IntegrationFlow integrationFlow( FunctionFlowBuilder functionFlowBuilder, JobLaunchingGateway jobLaunchingGateway ) {

		return
				functionFlowBuilder
						.fromSupplier("fileSupplier" )
						.split()
						.apply( "jobLaunchRequestFunction" )
						.log( LoggingHandler.Level.WARN )
						.handle( jobLaunchingGateway )
						.log()
						.nullChannel();
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Person> itemReader( @Value( "#{jobParameters['input.file.name']}" ) String resource )  {

		return new FlatFileItemReaderBuilder<Person>()
				.name( "personItemReader" )
				.resource( new FileSystemResource( resource ) )
				.delimited()
				.names( "firstName", "lastName" )
				.targetType( Person.class )
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Person> itemWriter(DataSource dataSource ) {

		return new JdbcBatchItemWriterBuilder<Person>()
				.dataSource( dataSource )
				.sql( "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)" )
				.beanMapped()
				.build();
	}

	@Bean
	Step step1(
			JobRepository jobRepository, PlatformTransactionManager transactionManager,
			ItemReader<Person> itemReader, ItemWriter<Person> itemWriter,
			TaskExecutor batchTaskExecutor
	) {

		return new StepBuilder( "step1", jobRepository )
				.<Person, Person> chunk( 3, transactionManager )
				.reader( itemReader )
				.writer( itemWriter )
				.taskExecutor(batchTaskExecutor)
				.build();
	}

	@Bean
	Job job( JobRepository jobRepository, Step step1 ) {

		return new JobBuilder( "job", jobRepository )
                .start( step1 )
				.build();
	}

	public record Person( String firstName, String lastName ) { }

}
