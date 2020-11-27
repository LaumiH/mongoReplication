Use the following command to start the configurator locally from the Writer-1.jar:

`docker run --rm -v Writer-1.jar:/home/Writer-1.jar -v config:/home/config -e LOGLEVEL=INFO --net mongodb_mongo-cluster --name config openjdk /bin/bash -c '/usr/bin/java -jar /home/Writer-1.jar'`