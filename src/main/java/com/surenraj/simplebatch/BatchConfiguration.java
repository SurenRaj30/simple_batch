package com.surenraj.simplebatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {
    @Autowired
    private HeartRepository heartRepository;
    private StepBuilder stepBuilder;

    //itemReader bean, will be using FlatItemReader to read from csv
    @Bean
    public FlatFileItemReader<Heart> reader() {
       FlatFileItemReader<Heart> itemReader = new FlatFileItemReader<Heart>();
       //get the file
       itemReader.setResource(new FileSystemResource("src/main/resources/heart_attack_dataset.csv"));
       //set step's name
       itemReader.setName("csvReader");
       //set which line to skip, for this csv, it will be the top header column
       itemReader.setLinesToSkip(1);
       itemReader.setLineMapper(lineMapper());
       return itemReader;
    }

    //itemProcessor bean
    //no business logic for now
    @Bean
    public HeartProcessor processor() {
        return new HeartProcessor();
    }

    //itemWriter bean
    @Bean
    public RepositoryItemWriter<Heart> writer() {
        RepositoryItemWriter<Heart> writer = new RepositoryItemWriter<Heart>();
        writer.setRepository(heartRepository);
        writer.setMethodName("save");
        return writer;
    }
    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("csv-step", jobRepository)
                .<Heart, Heart>chunk(10, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
    @Bean
    public Job job(JobRepository jobRepository, Step step1) {
        return new JobBuilder("importHeartData", jobRepository)
                .start(step1)
                .build();
    }

    private LineMapper<Heart> lineMapper() {
        DefaultLineMapper<Heart> lineMapper = new DefaultLineMapper<Heart>();

        //modifies the file
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        //will throw an exception if the number of fields doesn't match
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("gender", "age", "bloodPressure", "cholesterol", "hasDiabetes", "smokingStatus",
                "chestPain", "treatment");

        //maps the field to the entity's variables
        BeanWrapperFieldSetMapper<Heart> fieldSetMapper = new BeanWrapperFieldSetMapper<Heart>();
        fieldSetMapper.setTargetType(Heart.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }



}
