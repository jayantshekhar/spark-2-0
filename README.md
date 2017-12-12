# Spark

## Pre-requisites for Installation

Java installed on the laptop

## Download Spark 2.2.0

- Download Spark 2.2.0 from here : http://spark.apache.org/downloads.html

## Install Spark 2.2.0 on Mac

- tar -zxvf spark-2.2.0-bin-hadoop2.7.tgz

- export PATH=$PATH:/Users/path_to_downloaded_spark/spark-2.2.0-bin-hadoop2.7/bin

## Install Spark 2.2.0 on Windows

- Download Spark 2.2.0 from https://spark.apache.org/downloads.html

- If necessary download and install 7Zip program to extract .tgz files
 
- Extract spark-2.2.0-bin-hadoop2.7.tgz file
 
- Add the spark bin directory to Path : ...\spark-2.2.0-bin-hadoop2.7\bin
 
- Download/Clone WinUtils from https://github.com/steveloughran/winutils
 
- Copy all files under winutils\hadoop-2.7.1\bin from where WinUtils was extracted/cloned to the spark-2.2.0-bin-hadoop2.7\bin directory.
 
- Set the following Environment Variables (User level is fine):
  - HADOOP_HOME : to your spark-2.2.0-bin-hadoop2.7 directory
  - SPARK_HOME : to your spark-2.2.0-bin-hadoop2.7 directory
  
## Git

Nice to have

[IMPORTANT]: Downloads
Have the following downloaded before the session
* Spark binaries
* JDK installed

## Code

git clone https://github.com/jayantshekhar/spark-2-0
mvn clean package

## Execute

- hadoop fs -put README.md
- hadoop fs -put data

- spark2-submit --class org.workshop.rdd.FlightsRDD --master yarn --deploy-mode client --executor-memory 1G  --num-executors 1 --executor-cores 1 target/spark-exercises-1.0-jar-with-dependencies.jar

## IDE

- Install IntelliJ or Scala IDE for Eclipse
  - https://www.jetbrains.com/idea/download
  - http://scala-ide.org/
  
- Import this project as a Maven Project

## Exercise - RDD

### Churn Data

state	account_length	area_code	phone_number	intl_plan	voice_mail_plan	number_vmail_messages	total_day_minutes	total_day_calls	total_day_charge	total_eve_minutes	total_eve_calls	total_eve_charge	total_night_minutes	total_night_calls	total_night_charge	total_intl_minutes	total_intl_calls	total_intl_charge	number_customer_service_calls	churned

### Find

- Number of people per state
- Number of people per state who churned and who did not churn
- The first column indicates the state
- The last column is the churn column

## Exercise - Dataframes & Datasets

### Flights & Airport Data

### Find

- Find the number of flights per ORIGIN airport which are delayed by more than 10 minutes
- Find the total distance traveled per TAIL_NUM
- Find the number of flights per ORIGIN airport which are delayed by more than 10 minutes. Also display the airport name

## Exercies - Streaming

### Find

- Display all the flights delayed by more than 15 minutes





