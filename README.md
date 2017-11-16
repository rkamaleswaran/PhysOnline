# PhysOnline
Online Feature Extraction and Machine Learning of Streaming Physiological Data
<br> <i>Developed by Rishikesan Kamaleswaran and Jacob Sutton
<br> <i>Center for Biomedical Informatics, University of Tennessee Health Science Center

# Prerequisite Software:
  - Apache Spark 2.2.0
  - MongoDB 3.6
  - Amqp-client 4.1.0
  - SBT
  - Scala 2.1.1
  
# Build instructions
Extract directory, modify the credentials in the psprSpark.scala file in the source directory. Run "sbt package" in the root directory. Once the target file has been created use spark-submit to execute the spark file.

#Spark Submit
Dont forget jar files.
