## Start the dataflow

```bash

 mvn -e compile exec:java -Dexec.mainClass=org.zenika.com.beam.dataflow.sink.DataflowSink -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=ysamir-data-processing-test \
      --enableStreamingEngine \
      --inputSubscription=projects/ysamir-data-processing-test/subscriptions/input-data-sub\
      --outputTopic=projects/ysamir-data-processing-test/topics/output-data \
      --runner=DataflowRunner \
      --defaultWorkerLogLevel=INFO \
      --stagingLocation=gs://ysamir-data-processing-test_dataflow/staging/dataflowsink \
      --tempLocation=gs://ysamir-data-processing-test_dataflow/temp/dataflowsink \
      --outputRejectionBucket=gs://ysamir-data-processing-test_rejection_bucket \
      --maxNumWorkers=1 \
      --region=europe-west1"
```