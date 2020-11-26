import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
      if (stateStrToMatch.equalsIgnoreCase("primary"))
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
      //System.out.println(result.toJson());
    } catch (MongoCommandException mce) {
      if (mce.getErrorCodeName().contains("NotYetInitialized")) {
        System.err.println("The ReplicaSet has not been initialized yet!");
        return null;
      } else {
        mce.printStackTrace();
      }
    } catch (MongoTimeoutException mte) {
      System.err.println("The ReplicaSet has not been initialized yet!");
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
      System.out.println("Initiation of rs successful.\n" + result.toJson());
    } catch (MongoCommandException e) {
      System.out.println("The provided config is invalid. Provided config:\n" + config.toJson());
      System.err.println(e.getErrorCodeName());
      System.err.println(e.getResponse().toJson());
      result = null;
    }
    return result;
  }

  protected Document replSetReconfig(final MongoDatabase mongo, Document config) {
    //https://docs.mongodb.com/manual/reference/command/replSetReconfig/

    // https://stackoverflow.com/questions/50489692/adding-arrays-as-kubernetes-environment-variables
    // mount a js file into the container using a configmap
    // https://towardsdatascience.com/how-to-deploy-a-mongodb-replica-set-using-docker-6d0b9ac00e49?gi=e1409ea6e91
    /*
    replica.js
    rs.initiate({
      _id: 'rs0',
      members: [{
        _id: 0,
        host: 'mongo-0:27017'
      },
      {
        _id: 1,
        host: 'mongo-1:27017'
      }]
    })
    */
    /*
    create configmap from file and use in pod
    containers:
      - name: ...
        image: ...
        volumeMounts:
        - name: config-volume
          mountPath: /etc/config
    volumes:
      - name: config-volume
        configMap:
          name: app-config
    */

    // https://docs.mongodb.com/manual/tutorial/write-scripts-for-the-mongo-shell/
    // load("/data/db/scripts/myjstest.js")

    /*
    final BasicDBObject command = new BasicDBObject();
    command.put("eval", String.format("function() { %s return;}}, {entity_id : 1, value : 1, type : 1}).forEach(someFun); }", code));
    Document result = database.runCommand(command);
    */
    Document result;
    try {
      result = mongo.runCommand(new Document("replSetReconfig", config));
      System.out.println("Reconfig of rs successful.\n" + result.toJson());
    } catch (MongoCommandException e) {
      System.out.println("The provided config is invalid. Provided config:\n" + config.toJson());
      System.err.println(e.getErrorCodeName());
      System.err.println(e.getResponse().toJson());
      result = null;
    }
    return result;
  }

  protected boolean equalConfigs(MongoDatabase db, Document newConfig) {
    List<String> currentNames = new ArrayList<>();
    List<Document> members_document = replSetStatus(db).getList("members", Document.class);
    for (final Document member : members_document) {
      final String name = member.getString("name");
      currentNames.add(name);
    }

    List<String> newNames = new ArrayList<>();
    members_document = newConfig.getList("members", Document.class);
    for (final Document member : members_document) {
      final String name = member.getString("host");
      newNames.add(name);
    }

    Collections.sort(currentNames);
    Collections.sort(newNames);

    return currentNames.equals(newNames);
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
}
