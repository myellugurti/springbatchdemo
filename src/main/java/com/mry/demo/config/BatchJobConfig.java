package com.mry.demo.config;

import java.util.Date;

import javax.sql.DataSource;

import com.mry.demo.model.Product;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class BatchJobConfig {

	@Bean
	public ItemReader<Product> reader() {
		FlatFileItemReader<Product> flatFileItemReader = new FlatFileItemReader<>();
		flatFileItemReader.setResource(new ClassPathResource("productsFile.csv"));
		flatFileItemReader.setLineMapper(new DefaultLineMapper<Product>() {{
			setLineTokenizer(new DelimitedLineTokenizer() {{
				setDelimiter("|");
				setNames("productId","productCode","productCost");
			}});

			setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
				setTargetType(Product.class);
			}});
		}});
		return flatFileItemReader;
	}

	@Autowired
	private DataSource dataSource;

	@Bean
	public ItemWriter<Product> writer() {
		JdbcBatchItemWriter<Product> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
		jdbcBatchItemWriter.setSql("INSERT INTO PRODUCTS  (PID, PCODE , PCOST) VALUES (:productId, :productCode, :productCost )");
		jdbcBatchItemWriter.setDataSource(dataSource);
		jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		return jdbcBatchItemWriter;
	}


	@Bean
	public JobExecutionListener listener() {
		return new JobExecutionListener() {

			public void beforeJob(JobExecution je) {
				System.out.println("Startup - Started:" + je.getStatus() + " - " + new Date());
			}

			public void afterJob(JobExecution je) {
				System.out.println("End - Completed:" + je.getStatus() + " - " + new Date());
			}
		};

	}

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step")
				.<Product,Product>chunk(3)
				.reader(reader())
				.writer(writer())
				.build();
	}

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.listener(listener())
				.start(step())
				.build();
	}
	
}
