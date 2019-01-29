# mongodb-elastic

# Install Pysys 

Follow instructions at [https://sourceforge.net/projects/pysys/files//pysys/0.8.2/pysys-release.txt/view]

I have version 1.2.0. It uses python 2

# Install elastic python driver

pip2 install elasticsearch

# To create Test data

The following command inserts (-i 100) ~30KB docs (-f 1100) using 8 threads (-t 8) into the target cluster for 1 second (-d 1).

```java -jar bin/POCDriver.jar -c "mongodb+srv://user:pass@cluster.mongodb.net/test?retryWrites=true" -t 8 -o stats.out -d 1 -i 100 -f 1100 -e```

Unclear if 1 second will be enough - it depends on the environment where you run this but inserts are fast so 1 second might even be too much

