package com.broadcom.springconsulting.batch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.jdbc.JdbcTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Import( TestcontainersConfiguration.class )
@SpringBootTest
@SpringBatchTest
class BatchDemoApplicationTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private JobRepositoryTestUtils jobRepositoryTestUtils;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private FlatFileItemReader<BatchDemoApplication.Person> itemReader;

	@Autowired
	JdbcBatchItemWriter<BatchDemoApplication.Person> itemWriter;

	@TempDir
	Path tempDir;

	Path inputFile;

	@BeforeEach
	public void setup( @Autowired Job jobUnderTest ) {

		this.jobLauncherTestUtils.setJob( jobUnderTest );
		this.jobRepositoryTestUtils.removeJobExecutions();

		this.inputFile = this.tempDir.resolve( "input-test-data.csv" );

	}

	@AfterEach
	void teardown() {

		this.jdbcTemplate.update( "DELETE FROM people" );

	}

	@Test
	void contextLoads() {
	}

	@Test
	void testJob() throws Exception {

		Files.write( this.inputFile, Stream.of( "Daniel,Frey", "Stephanie,Frey" ).toList() );

		var jobParameters = new JobParametersBuilder()
				.addString("fileName", "file://" + this.inputFile.toString() )
				.toJobParameters();

		var jobExecution = this.jobLauncherTestUtils.launchJob( jobParameters );

		assertThat( jobExecution.getExitStatus() ).isEqualTo(ExitStatus.COMPLETED );

	}

	@Test
	void testStep1() throws Exception {

		Files.write( this.inputFile, Stream.of( "Daniel,Frey" ).toList() );

		var jobParameters = new JobParametersBuilder()
				.addString("fileName", "file://" + this.inputFile.toString() )
				.toJobParameters();

		var stepExecution = MetaDataInstanceFactory.createStepExecution( jobParameters );

		StepScopeTestUtils.doInStepScope( stepExecution, () -> {

			this.itemReader.open( stepExecution.getExecutionContext() );

			var actual = this.itemReader.read();

			var expected = new BatchDemoApplication.Person( "Daniel", "Frey" );
			assertThat( actual ).isEqualTo( expected );

			this.itemReader.close();

			this.itemWriter.write( new Chunk<>(actual ) );
			var inserted = JdbcTestUtils.countRowsInTable( this.jdbcTemplate, "people" );
			assertThat( inserted ).isEqualTo( 1 );

			return null;
		});

	}

}
