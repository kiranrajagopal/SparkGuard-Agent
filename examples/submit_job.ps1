# Simulated Livy submit script with embedded Spark config
$aadToken = $env:AAD_TOKEN

$LivyRequest = @'
{
  "file": "hdfs:///projects/MyApp/app.jar",
  "className": "com.example.SparkJob",
  "name": "my-spark-job",
  "driverMemory": "2g",
  "executorMemory": "4g",
  "numExecutors": 30,
  "conf": {
    "spark.executor.memoryOverhead": "512m",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.shuffle.partitions": "10000",
    "spark.memory.storageFraction": "0.8",
    "spark.speculation": "true"
  }
}
'@

$headers = @{
    "Authorization" = "Bearer $aadToken"
    "Content-Type"  = "application/json"
}

Invoke-RestMethod -Uri "https://livy.example.com/batches" -Method Post -Headers $headers -Body $LivyRequest
