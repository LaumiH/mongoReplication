import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import java.util.ArrayList;
import org.bson.Document;

public class Writer {

  private void constantWrite() {
    if(Main.POD_NAME.isEmpty()) {
      System.out.println("Pod name must be specified as ENV!");
      System.exit(1);
    }

    //System.out.println("Connected to the database successfully");

    MongoDatabase db = Main.mongoClient.getDatabase("example");
    if (!db.listCollectionNames().into(new ArrayList<String>()).contains("test_" + Main.POD_NAME)) {
      db.createCollection("test_" + Main.POD_NAME);
      System.out.println("Collection 'test_" + Main.POD_NAME + "' created successfully");
    } else {
      System.out.println("Using existing collection 'test_" + Main.POD_NAME + "'");
    }

    MongoCollection<Document> collection;

    for(;;) {
      try {
        collection = db.getCollection("test_" + Main.POD_NAME);
        System.out.println("Collection 'test_" + Main.POD_NAME + "' selected successfully");

        FindIterable<Document> iterable = collection.find();
        if (iterable.first() == null) {
          Document document1 = new Document("title", "MongoDB")
            .append("description", "database")
            .append("likes", 200)
            .append("url", "http://www.tutorialspoint.com/mongodb/")
            .append("by", "tutorials point");
          collection.insertOne(document1);
        }

        // Retrieving a collection
        /*iterable = collection.find();
        for (Document document : iterable) {
          System.out.println(document);
        }*/

        for(int i = 0; i<100; i++) {
          System.out.print("Querying MongoDB likes ... ");
          Document document1 = collection.find(new BasicDBObject("title", "MongoDB")).projection(
            Projections.fields(Projections.include("likes"), Projections.excludeId())).first();
          int likes = document1.getInteger("likes");
          System.out.println(likes + " likes");

          Thread.sleep(1000);

          collection.updateOne(Filters.eq("title", "MongoDB"), Updates.set("likes",
            likes + 1));

          document1 = collection.find(new BasicDBObject("title", "MongoDB")).projection(
            Projections.fields(Projections.include("likes"), Projections.excludeId())).first();
          System.out.println("MongoDB likes updated to ... " + document1.getInteger("likes"));

          Thread.sleep(1000);
        }

        collection.deleteOne(Filters.eq("title", "MongoDB"));
        System.out.println("Document deleted successfully...");

        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
