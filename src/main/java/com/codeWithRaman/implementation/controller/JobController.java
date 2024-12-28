package com.codeWithRaman.implementation.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

	private final JobLauncher jobLauncher;
	private final Job job;
	public JobController(JobLauncher jobLauncher, Job job) {
		this.jobLauncher = jobLauncher;
		this.job = job;
	}

	@GetMapping("/run-job")
	public ResponseEntity<String> runJob(){
		try {
		JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.toJobParameters();
		jobLauncher.run(job, jobParameters);
		return ResponseEntity.ok("Batch Job Has been triggered ");
		}catch (Exception e) {
			return ResponseEntity.status(500).body("Job Execution failed"+e.getMessage());
		}
	}
	
}
