# Apache Airflow Project using Spark (`SparkSubmitOperator`)

Hi everyone! I created this project to simulate an ETL process using Apache Airflow that can integrate Spark processing and receiving/sending data to AWS S3 buckets.

## **1. Project description**:
In my AWS S3 bucket, I have 3 datasets related to FIFA World Cup history (until Brazil 2014 World Cup). The idea is extract these datasets using spark, transforming the data and loading into new datasets that are going to be the inputs for a dashboard developed with AWS Quicksight.

You can find the datasets that I stored in my S3 bucket on the /project-datasets folder

## **2. Configuring Airflow Workers to run Spark tasks**:

In this case, I'm using Astronomer to create my Airflow project, to complete my project, I needed to set up a new *provider*: the `apache-airflow-providers-apache-spark` provider.

First, we need to create our astro project:

```bash
astro dev init
```

To accomplish this, I needed to do the following steps:

### a. <ins>Configuring JAVA_HOME environment variable in the Airflow Executors</ins>:

Add the following packages in your *packages.txt* file:

```
libpq-dev
gcc
default-jre
default-jdk
```

In your Docker File add the following:

```bash
ENV JAVA_HOME /usr/lib/jvm/java-1.11.0-openjdk-amd64
```

The file name will depend on the java version is installed in your Airflow Executors.

### b. <ins>Installing Apache Spark Provider</ins>:

Add the following package name into your *Requirements.txt* file:

```bash
apache-airflow-providers-apache-spark==4.0.0
```

### c. <ins>Adding your AWS Developer Credentials</ins>:

Add the following lines into your *DockerFile* to add your enviroment variables:

```bash
ENV AWS_ACCESS_KEY  '[YOUR_AWS_ACCESS_KEY]'
ENV AWS_SECRET_ACCESS_KEY '[YOUR_AWS_SECRET_ACCESS_KEY]'
```

### d. <ins>Including AWS Jars for Spark Processing</ins>:

On your /include folder inside your **local** project folder, add the following jar files:

```bash
hadoop-aws-3.2.2.jar
aws-java-sdk-1.12.368.jar
aws-java-sdk-bundle-1.12.368.jar
```

You can download them in the following links:
- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.2.2
- https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.12.368
- https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.12.368

After putting this jar on your local folder, using the *DockerFile* we will send them to the Executors folder. Just add the following lines into your *DockerFile*:

```bash
RUN rm -rf /include/*
COPY /include/ /include
```

### d. <ins>Moving your Python files</ins>:

In your **local** project folder, I save my Python foles in the /app folder. The idea is to move this files to the /app folder in our executors. To do that just add the following lines into your *DockerFile*:

```bash
RUN rm -rf /app/*
COPY /app/ /app
```

Finally, because we are storing parquet files in our Executor, I needed to change the permissions.

If you are developing a personal project, you can just put a 777 permissions.

Add these lines into your *DockerFile*:

```bash
USER root
RUN chmod -R 777 /app
```

Finally, start your Airflow service:
```bash
astro dev start
```

Hope this projects helps!