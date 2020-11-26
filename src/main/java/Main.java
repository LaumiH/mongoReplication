import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.json.JsonParseException;

public class Main {
  public static String CONNECTION_STRING;
  public static String POD_NAME;
  public static MongoClient mongoClient;

  public static void main(String args[]) throws InterruptedException {

    List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
    loggers.add(LogManager.getRootLogger());
    for ( Logger logger : loggers ) {
      logger.setLevel(Level.OFF);
    }

    // https://towardsdatascience.com/how-to-deploy-a-mongodb-replica-set-using-docker-6d0b9ac00e49?gi=e1409ea6e91
    // https://www.sohamkamani.com/blog/2016/06/30/docker-mongo-replica-set/
    // https://stackoverflow.com/questions/35567277/run-my-mongodb-command-from-java-program

    // docker run --rm -v /home/laumi/Kubernetes/MongoDB/Writer/target:/home --net mongo-cluster openjdk /bin/bash -c '/usr/bin/java -jar /home/Writer-1.jar'
    // docker run --rm -p 30003:27017 --name mongo-2 --net mongo-cluster -d  mongo --replSet 'rs0' --bind_ip 0.0.0.0
    // config = { "_id" : "rs0", "members" : [ {"_id" : 0, "host" : "127.19.0.2:27017"}, {"_id" : 1, "host" : "127.19.0.3:27017"}, {"_id" : 2, "host" : "127.19.0.4:27017"} ] }
    // rs.initiate(config = { "_id" : "rs0", "members" : [ {"_id" : 0, "host" : "mongo-0:27017"}, {"_id" : 1, "host" : "mongo-1:27017"}, {"_id" : 2, "host" : "mongo-2:27017"} ]


    /*CONNECTION_STRING = System.getenv("CONNECTION_STRING") != null ?
      (System.getenv("UUID")) :
      "mongodb://mongo-0.gke-0.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-1.gke-0.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-2.gke-0.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-0.gke-1.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-1.gke-1.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-2.gke-1.mongo.mongo.svc.clusterset.local:27017,"
        + "mongo-2-0.gke-0.mongo-2.mongo-2.svc.clusterset.local:27017"
        + "/?replicaSet=set0";
    */

    //POD_NAME = System.getenv("POD_NAME");

    //CONNECTION_STRING = "mongodb://127.19.0.2:30001,127.19.0.3:30002,127.19.0.4:30003/?replicaSet=rs0"; // geht, aber kann dann mongo-0 hostnames nicht resolven in mongo-cluster network
    //CONNECTION_STRING = "mongodb://mongo-0:27017,mongo-1:27017,mongo-2:27017/?replicaSet=rs0";
    //mongoClient = new MongoClient(new MongoClientURI(CONNECTION_STRING));

    //MongoDatabase db = Main.mongoClient.getDatabase("admin");
    /*if (!db.listCollectionNames().into(new ArrayList<String>()).contains("test_" + POD_NAME)) {
      db.createCollection("test_" + POD_NAME);
      System.out.println("Collection 'test_" + POD_NAME + "' created successfully");
    } else {
      System.out.println("Using existing collection 'test_" + POD_NAME + "'");
    }*/

    Configurator configurator = new Configurator();
    Document config;

    // TODO: what happens if ConfigMap is changed when program is down?
    //  The connection string derived from config will not match the ReplicaSet state
    //  Instances too much do not matter
    //  Not having the primary in the connection string does not matter

    for (;;) {
      String configfile = "";
      try {
        configfile = new Scanner(new File("/home/resources/config")).useDelimiter("\\Z").next();
        config = Document.parse(configfile);
      } catch (FileNotFoundException fnfe) {
        fnfe.printStackTrace();
        Thread.sleep(10000);
        continue;
      } catch (JsonParseException json) {
        System.out.println("The provided config is invalid. Provided config:\n" + configfile);
        Thread.sleep(10000);
        continue;
      }

      CONNECTION_STRING = "mongodb://";
      String firstMember = null;
      List<Document> members_document = config.getList("members", Document.class);
      List<String> members = new ArrayList<>();
      for (final Document member : members_document) {
        String hostnameAndPort = member.getString("host");
        members.add(hostnameAndPort);
        if (firstMember  == null) firstMember = hostnameAndPort;
        CONNECTION_STRING += hostnameAndPort + ",";
      }
      CONNECTION_STRING = CONNECTION_STRING.substring(0, CONNECTION_STRING.length()-1) + "/?replicaSet=rs0";
      System.out.println("\n" + CONNECTION_STRING);

      // try to connect to connection string, without knowing replication is set up
      try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTION_STRING))) {
        MongoDatabase db = mongoClient.getDatabase("admin");
        if (configurator.isReplicaSet(db)) {
          // we have a configured replica set
          if (configurator.getInvalidReplicaSetSize(db) > 0) {
            System.out.println("There are unhealthy members!\n"
              + "Checking if they are removed from replica set by config update ...");
            if (members.stream().anyMatch(configurator.getInvalidMembers(db)::contains)) {
              System.out.println("... Unhealthy members won't be removed by config update (completely).\n"
                + "Unhealthy members:");
              for (String member : configurator.getInvalidMembers(db)) {
                System.out.println("\t\t" + member);
              }
              System.out.println("Restore health of mongo cluster before updating replica set config, "
                + "or change the config.\n"
                + "Will pause for 30 sec.");
              Thread.sleep(30000);
              continue;
            }
            System.out.println("... Unhealthy members will be removed by config update. Continuing.");
          }
          // every member is reachable, so check if there is a primary
          assert !configurator.getPrimaryAsString(db).isEmpty();
          // compare config to current config
          if (!configurator.equalConfigs(db, config)) {
            System.out.println("Reconfiguring replica set.");
            // increment the version
            config.replace("version", configurator.getVersionNumber(db) + 1);
            configurator.replSetReconfig(db, config);
          } else {
            System.out.println("Nothing has changed :)");
          }
        } else {
          // this is no replica set yet
          System.out.println("Setting up new Replication using\n" + config.toJson());
          try (MongoClient mongoClientInit = new MongoClient(new MongoClientURI("mongodb://" + firstMember))) {
            db = mongoClientInit.getDatabase("admin");
            configurator.replSetInitiate(db, config);
          } catch (Exception e) {
            System.out.println("Not able to connect to instance " + firstMember + "! Is the hostname and port correct?");
          }
        }
      } catch (Exception e) {
        System.out.println("Cannot properly connect to connection string, retrying.");
        e.printStackTrace();
      } finally {
        Thread.sleep(10000);
      }
    }
  }
}
