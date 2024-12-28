package com.codeWithRaman.implementation.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.codeWithRaman.implementation.model.Users;

@Configuration
public class BatchConfig {
	
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final DataSource dataSource;
	public BatchConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			DataSource dataSource) {
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.dataSource = dataSource;
	}
	
	@Bean
	public FlatFileItemReader<Users> reader(){
		 return new FlatFileItemReaderBuilder<Users>()
			        .name("csvReader")
			        .resource(new ClassPathResource("users.csv"))
			        .linesToSkip(1) 
			        .delimited()
			        .names("firstName", "lastName", "email", "age") 
			        .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
			            setTargetType(Users.class); 
			        }})
			        .build();
	}
	
	@Bean
	public ItemProcessor<Users, Users> processor(){
		return user-> {
			user.setFirstName(user.getFirstName().toUpperCase());
			return user;
		};
	}

	@Bean
	public JdbcBatchItemWriter<Users> writer(){
		return new JdbcBatchItemWriterBuilder<Users>().dataSource(dataSource)
				.sql("INSERT INTO users (first_name, last_name, email, age) VALUES (:firstName, :lastName, :email, :age)")
				.beanMapped().build();
	}
	
	@Bean 
	public Step step() {
		return new StepBuilder("csv-to-database-step", jobRepository).<Users,Users>chunk(10,transactionManager).reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}
	
	@Bean
	public Job job() {
		return new JobBuilder("csv-to-database-job", jobRepository).start(step()).build();
	}
}
