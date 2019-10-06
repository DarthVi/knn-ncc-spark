# kNN and NCC on Spark
This is an implementation of the k-Nearest-Neighbour and Nearest Centroid Classifier
on Spark using Scala. The implementation has been tested on the HIGGS dataset, which is available
[here](https://archive.ics.uci.edu/ml/datasets/HIGGS).

The code has been developed as a project for the exam of the course "Scalable and Cloud Programming" held in 
Alma Mater University of Bologna.

## Creating the JAR
Open the terminal, clone the repository via `git clone https://github.com/DarthVi/knn-ncc-spark.git`, then
type `cd knn-ncc-spark` to enter inside the cloned directory. Finally type `sbt clean package` to create the the JAR file.
You can find the created JAR inside the folder `knn-ncc-spark/target/scala-2.11/`.

## Deployment and execution on the cloud
The code has been tested using an Amazon EMR cluster.
To deploy it, upload the training set, the test set, the JAR file and the `classifier.properties` file
on an Amazon S3 bucket.

Then run the Amazon EMR instance, ssh into the machine and run the following commands (substitute the code inside
the brackets with your own file paths):
```
aws s3 cp s3://<bucket-path-to-the-jar> .
aws s3 cp s3://<bucket-path-to-property-file> .
```

After executing this command, you should find the JAR file and the `classifier.properties` file
inside the directory where the commands have been executed.

Open the `classifier.properties` file via nano or vi and insert the correct
paths for the training set (on the line `classifier.filepath`) and test set (on the line `classifier.testpath`).
If you've correctly placed the training set .txt and test set .txt on an Amazon S3 bucket, these paths must begin with `s3n://`.

To execute the code, simply type `spark-submit <path-to-jar>`. 