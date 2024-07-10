# ETL off an SQS Queue

The goal of this project is to demonstrate the ability to create a small application that reads JSON data from an AWS SQS Queue, processes and transforms the data, and then writes the transformed data to a Postgres database. The entire process will be run locally using Docker.

## Deployment

The following steps would be implemented to make sure the application is ready for production:

1. Set Up The Environment: This includes choosing AWS as a cloud provider. This is an obvious choice since SQS and Postgres are being utilized. Additionally, a VPC would be created for additional security.

2. Service Setup: This step includes setting up the SQS queue and Postgres database. The utilization of Amazon SQS and Amazon RDS is paramount to setup the Queue and Postgres database.

3. Containerize the application: Docker would be used to containerize the application.

4. Deploy the containers: Amazon ECS or EKS would run the docker containers, while Fargate would be used to manage the containers.

## Additional components for Production

1. Improve Security: Firewalls, encryption, and audit logging would be implemented to make the application more secure.

2. Create Monitoring and Logging procedures: This includes implementing centralized logging using ELK or AWS Cloudwatch

3. Create Application Performance Management: This can be easily done using AWS X-Ray.

These are just a few additional components that I would implement to get this application ready for deployment. There are obviously more, but these three are the most important.

## Scaling with a growing dataset

This application can scale multiple ways with a growing dataset. The main scaling that would need to occur would be: pipeline, database, and application scaling. These revolve around the main functionality of the application.

1. Pipeline Scaling: Queues and data storage would need to be scaled with a growing dataset. Kinesis can be used to process datastreams in real time and handle high data ingestion. Amazon S3 can be used for its enhanced storage and data lake capabilities.

2. Database Scaling: Vertical and horizontal scaling would need to occur with a growing dataset. To increase vertical scalability, one can use larger instances, and to increase horizontal scalability one can use sharding.

3. Application Scalability: Kubernates can be used to manage and scale containers automatically. Additionally, Load balancing can be implemented in cases of high traffic.

## Recovering PII Data

In my application, I utilized encryption to mask PII data. To recover the data, the data would need to be decrypted in order to recover. This would only be done when uncovering the data is necessary.

## Assumptions

1. JSON Data Consistency
2. PII Handling can be done using basic Encryption
3. Data Volume is Manageable