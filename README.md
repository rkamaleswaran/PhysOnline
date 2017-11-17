# PhysOnline
Online Feature Extraction and Machine Learning of Streaming Physiological Data
<br> This is a novel near real-time machine learning pipline used to classify Paroxysmal artial fibrillation (PAF) in an online streaming envoriment. This methid uses the scalable and parallelized Apache Spark platform.
<br> 
<br> <i>Developed by Rishikesan Kamaleswaran and Jacob Sutton
<br> <i>Center for Biomedical Informatics, University of Tennessee Health Science Center

# Prerequisite Software:
  - Apache Spark 2.2.0
  - MongoDB 3.6
  - Amqp-client 4.1.0
  - SBT
  - Scala 2.1.1
  
# Build instructions
  1) Extract directory.
  2) Modify the credentials in the psprSpark.scala file in the source directory. You will need to alter the credentials for the RabbitMQ connection.
  3) Run "sbt package" in the root directory. No errors should appear.
  4) Once the step #3 has been run, a target file should have been created. Then use the command "spark-submit" in the root directory to execute the spark file.

# Spark Submit
The spark-submit command will not work unelss you have have listed the coinciding jar files to be run in tandem:
   - amqp-client-4.1.0.jar
   - spark-rabbitme-0.5.1.jar
   - akka-actor_2.11-2.4.11.jar
   - config-1.3.1.jar
   - slf4j-nop-1.7.25.jar
   - mongo-spark-connector_2.11-2.2.0.jar
   - mongo-java-driver-3.4.2.jar  
<br>These can all be found in the in the lib directory.
For more information on how the spark streaming process works, please check out https://spark.apache.org/docs/latest/streaming-programming-guide.html
