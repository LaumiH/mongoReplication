import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;

public class Configurator {
  protected boolean isReplicaSet(MongoDatabase mongo) {
    return replSetStatus(mongo) != null;
  }

  protected String getPrimaryAsString(MongoDatabase mongo) {
    return getMemberNameByState(mongo, "primary").get(0);
  }

  protected List<String> getSecondariesAsString(MongoDatabase mongo) {
    return getMemberNameByState(mongo, "secondary");
  }

  protected boolean isPrimary(MongoDatabase mongo) {
    return isMaster(mongo).getBoolean("ismaster");
  }

  protected String deriveConnectionString(Document config, String repset) {
    String connectionString = "mongodb://";
    for (String member : getMembersFromConfig(config)) {
      connectionString += member + ",";
    }
    return connectionString.substring(0, connectionString.length()-1) + "/?replicaSet=" + repset;
  }

  private List<String> getMemberNameByState(MongoDatabase mongo, String stateStrToMatch) {
    assert replSetStatus(mongo) != null;
    List<String> filtered = new ArrayList<>();
    List<Document> members_document = replSetStatus(mongo).getList("members", Document.class);
    for (final Document member : members_document) {
      String hostnameAndPort = member.getString("name");
      if (!hostnameAndPort.contains(":"))
        hostnameAndPort = hostnameAndPort + ":27017";
      final String stateStr = member.getString("stateStr");
      if (stateStr.equalsIgnoreCase(stateStrToMatch))
        filtered.add(hostnameAndPort);
      if (stateStr.equalsIgnoreCase("primary"))
        return filtered;
    }
    if (filtered.isEmpty())
      throw new IllegalStateException("No member found in state " + stateStrToMatch);
    return filtered;
  }

  protected int getValidReplicaSetSize(MongoDatabase mongo) {
    assert replSetStatus(mongo) != null;
    int size = 0;
    List<Document> members_document = replSetStatus(mongo).getList("members", Document.class);
    for (final Document member : members_document) {
      final String stateStr = member.getString("stateStr");
      if (stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY"))
        size++;
    }
    return size;
  }

  protected int getInvalidReplicaSetSize(MongoDatabase mongo) {
    assert replSetStatus(mongo) != null;
    int invalid = 0;
    List<Document> members_document = replSetStatus(mongo).getList("members", Document.class);
    for (final Document member : members_document) {
      invalid++;
      final String stateStr = member.getString("stateStr");
      if (stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY")) invalid--;
    }
    return invalid;
  }

  protected List<String> getInvalidMembers(MongoDatabase mongo) {
    assert replSetStatus(mongo) != null;
    List<String> invalid = new ArrayList<>();
    List<Document> members_document = replSetStatus(mongo).getList("members", Document.class);
    for (final Document member : members_document) {
      final String stateStr = member.getString("stateStr");
      if (!(stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY")))
        invalid.add(member.getString("name"));
    }
    return invalid;
  }

  private Document replSetStatus(final MongoDatabase mongo) {
    // Check to see if this is a replica set... if not, get out of here.
    Document result = null;
    try {
      result = mongo.runCommand(new Document("replSetGetStatus", 1));
    } catch (MongoCommandException mce) {
      if (mce.getErrorCodeName().contains("NotYetInitialized")) {
        Main.LOGGER.info("NotYetInitialized: The ReplicaSet has not been initialized yet.");
        return null;
      } else {
        mce.printStackTrace();
      }
    } catch (MongoTimeoutException mte) {
      // this can mean the set is not initialized, OR the replica set name is different than the initialized one!!
      // I did not find any other valid way than to query each member inside the config for a rs
      String memberList = StringUtils.substringBetween(Main.CONNECTION_STRING, "//", "/");
      for (String member : memberList.split(",")) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://" + member))) {
          MongoDatabase db = mongoClient.getDatabase("admin");
          if (isReplicaSet(db)) {
            result = replSetStatus(db);
            Main.LOGGER.severe("ReplicaSet has been initialized with name " + result.getString("set") + "!");
            Main.LOGGER.severe("ReplicaSet name of new config does not match old config, aborting!");
            return result;
          }
        }
      }
      Main.LOGGER.info("MongoTimeoutException: The set has not been initialized yet.");
      return null;
    }
    return result;
  }

  private Document isMaster(final MongoDatabase mongo) {
    assert replSetStatus(mongo) != null;
    return mongo.runCommand(new Document("isMaster", 1));
  }

  protected Document replSetInitiate(final MongoDatabase mongo, Document config) {
    //https://docs.mongodb.com/manual/reference/command/replSetInitiate/
    Document result;
    try {
      result = mongo.runCommand(new Document("replSetInitiate", config));
      Main.LOGGER.info("Initiation of rs successful.\n" + result.toJson());
    } catch (MongoCommandException e) {
      Main.LOGGER.severe(e.getErrorCodeName());
      Main.LOGGER.severe("The provided config is invalid. Provided config:\n" + config.toJson());
      Main.LOGGER.severe(e.getResponse().toJson());
      result = null;
    }
    return result;
  }

  protected Document replSetReconfig(final MongoDatabase mongo, Document config) {
    Document result;
    try {
      result = mongo.runCommand(new Document("replSetReconfig", config));
      Main.LOGGER.info("Reconfig of rs successful.\n" + result.toJson());
    } catch (MongoCommandException e) {
      Main.LOGGER.severe(e.getErrorCodeName());
      Main.LOGGER.severe("The provided config is invalid. Provided config:\n" + config.toJson());
      Main.LOGGER.severe(e.getResponse().toJson());
      result = null;
    }
    return result;
  }

  protected boolean equalConfigs(MongoDatabase db, Document newConfig) {
    Document replSetStatus = replSetStatus(db);
    String currentRsName = replSetStatus.getString("set");
    String newRsName = newConfig.getString("_id");
    if (!currentRsName.equals(newRsName)) {
      Main.LOGGER.severe("ReplicaSet name of new config does not match old config, aborting!");
    } else {
      List<String> currentNames = getMembersFromConfig(replSetStatus);
      List<String> newNames = getMembersFromConfig(newConfig);
      Collections.sort(currentNames);
      Collections.sort(newNames);
      return currentNames.equals(newNames);
    }
    return true;
  }

  protected int getVersionNumber(MongoDatabase db) {
    List<Document> members_document = replSetStatus(db).getList("members", Document.class);
    int version = 1;
    for (final Document member : members_document) {
      int v = member.getInteger("configVersion");
      if (v > version) version = v;
    }
    return version;
  }

  protected List<String> getMembersFromConfig(Document config) {
    List<String> members = new ArrayList<>();
    List<Document> members_document = config.getList("members", Document.class);
    String key = "name";
    if (members_document.get(0).getString(key) == null) key = "host";
    for (final Document member : members_document) {
      members.add(member.getString(key));
    }
    return members;
  }

  protected boolean wouldDeletePrimary(MongoDatabase db, Document newConfig) {
    String primary = getPrimaryAsString(db);
    Main.LOGGER.info("Primary: " + primary + ", members: ");
    List<String> newMembers = getMembersFromConfig(newConfig);
    for (String s : newMembers) {
      Main.LOGGER.info("\t" + s);
    }
    boolean result = !newMembers.contains(primary);
    if (result) Main.LOGGER.severe("The primary currently is: " + primary);
    return result;
  }
}
