import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.json.JsonParseException;

public class Main {
  public static String CONNECTION_STRING;
  public static String REPLICASET;
  public static String POD_NAME;
  public static MongoClient mongoClient;

  public final static Logger LOGGER = Logger.getLogger(Main.class.getName());

  public static void main(String args[]) throws InterruptedException {

    String LOGLEVEL = System.getenv("LOGLEVEL") != null ? System.getenv("LOGLEVEL") : "INFO";

    System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    LOGGER.setLevel(Level.parse(LOGLEVEL));

    // https://towardsdatascience.com/how-to-deploy-a-mongodb-replica-set-using-docker-6d0b9ac00e49?gi=e1409ea6e91
    // https://www.sohamkamani.com/blog/2016/06/30/docker-mongo-replica-set/
    // https://stackoverflow.com/questions/35567277/run-my-mongodb-command-from-java-program

    // docker run --rm -v /home/laumi/Kubernetes/MongoDB/Writer/target:/home --net mongo-cluster openjdk /bin/bash -c '/usr/bin/java -jar /home/Writer-1.jar'
    // docker run --rm -p 30003:27017 --name mongo-2 --net mongo-cluster -d  mongo --replSet 'rs0' --bind_ip 0.0.0.0

    //POD_NAME = System.getenv("POD_NAME");

    //MongoDatabase db = Main.mongoClient.getDatabase("admin");
    /*if (!db.listCollectionNames().into(new ArrayList<String>()).contains("test_" + POD_NAME)) {
      db.createCollection("test_" + POD_NAME);
      System.out.println("Collection 'test_" + POD_NAME + "' created successfully");
    } else {
      System.out.println("Using existing collection 'test_" + POD_NAME + "'");
    }*/

    Configurator configurator = new Configurator();
    Document config;

    // TODO: what happens if ConfigMap is changed when this program is down?
    //  The connection string derived from config will not match the ReplicaSet state
    //  Instances too much do not matter
    //  Not having the primary in the connection string does not matter

    for (;;) {
      LOGGER.info("Reading config.");
      String configfile = "";
      try {
        // TODO: read config file from ConfigMap

        configfile = new Scanner(new File("/home/config")).useDelimiter("\\Z").next();
        config = Document.parse(configfile);
      } catch (FileNotFoundException fnfe) {
        LOGGER.severe("The config file cannot be accessed.");
        Thread.sleep(5000);
        continue;
      } catch (JsonParseException json) {
        LOGGER.severe("The provided config is invalid. Provided config:\n" + configfile);
        Thread.sleep(5000);
        continue;
      }

      REPLICASET = config.getString("_id");
      CONNECTION_STRING = configurator.deriveConnectionString(config, REPLICASET);
      String firstMember = StringUtils.substringBetween(CONNECTION_STRING, "//", ",");
      LOGGER.info("Connection string: " + CONNECTION_STRING);
      LOGGER.info("First member: " + firstMember);

      // try to connect to connection string, without knowing replication is set up
      try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTION_STRING))) {
        MongoDatabase db = mongoClient.getDatabase("admin");
        if (configurator.isReplicaSet(db)) {
          LOGGER.info("ReplicaSet already initialized.");
          // we have a configured replica set
          // check for unreachable members and if found abort configuration until replica set is healthy
          if (configurator.getInvalidReplicaSetSize(db) > 0) {
            LOGGER.warning("There are unhealthy members!\n"
              + "Checking if they are removed from replica set by config update ...");
            if (configurator.getMembersFromConfig(config).stream().anyMatch(configurator.getInvalidMembers(db)::contains)) {
              LOGGER.warning("Unhealthy members won't be removed by config update (completely).\n"
                + "Unhealthy members:");
              for (String member : configurator.getInvalidMembers(db)) {
                LOGGER.warning("\t\t" + member);
              }
              LOGGER.severe("There are unhealthy members. Restore health of mongo cluster before updating replica set config, "
                + "or change the config.\n"
                + "Will pause for 30 sec.");
              Thread.sleep(5000);
              continue;
            }
            LOGGER.warning("Unhealthy members will be removed by config update. Continuing.");
          }
          // every member is reachable, so check if there is a primary (there must be if everything is healthy, but anyways)
          if (configurator.getPrimaryAsString(db).isEmpty()) {
            LOGGER.severe("ReplicaSet has no primary instance, aborting config change!");
            Thread.sleep(5000);
            continue;
          }
          // check if the new config would delete the primary instance
          if (configurator.wouldDeletePrimary(db, config)) {
            LOGGER.severe("New config would delete the primary instance, aborting config change!");
            Thread.sleep(5000);
            continue;
          }

          // compare config to current config
          if (!configurator.equalConfigs(db, config)) {
            LOGGER.info("Reconfiguring replica set.");
            // increment the version
            config.replace("version", configurator.getVersionNumber(db) + 1);
            configurator.replSetReconfig(db, config);
          } else {
            LOGGER.info("Nothing has changed.");
          }
        } else {
          // this is no replica set yet
          LOGGER.info("Setting up new Replication using\n" + config.toJson());
          try (MongoClient mongoClientInit = new MongoClient(new MongoClientURI("mongodb://" + firstMember))) {
            db = mongoClientInit.getDatabase("admin");
            configurator.replSetInitiate(db, config);
          } catch (Exception e) {
            LOGGER.severe("Not able to connect to instance " + firstMember + "! Is the hostname and port correct?");
            Thread.sleep(5000);
          }
        }
      } catch (Exception e) {
        LOGGER.severe("Cannot properly connect to connection string, retrying.");
        e.printStackTrace();
      } finally {
        Thread.sleep(10000);
      }
    }
  }
}
