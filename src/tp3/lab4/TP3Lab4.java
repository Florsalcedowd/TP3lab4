/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp3.lab4;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author flocy
 */
public class TP3Lab4 {

    public static void main(String[] args) {

        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);

        String uri = "mongodb+srv://admin:admin@tup2020-87uxq.mongodb.net/test?retryWrites=true&w=majority";
        MongoClientURI clientURI = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(clientURI);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("paises_db");
        MongoCollection<Document> collection = mongoDatabase.getCollection("paises");

        Document document = new Document();

        String restUrl = "https://restcountries.eu/rest/v2/callingcode/";

        JSONParser parser = new JSONParser();

        for (int codigo = 1; codigo <= 300; codigo++) {

            try {

                URL rutaJson = new URL(restUrl + codigo); 
                System.out.println(rutaJson);
                
                URLConnection yc = rutaJson.openConnection();
                
                BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
                
                JSONArray a = (JSONArray) parser.parse(in.readLine());
                
                if (a != null) {
                    for (Object object : a) {
                        JSONObject paisJson = (JSONObject) object;
                        document.append("codigoPais", codigo);
                        document.append("nombrePais", paisJson.get("name"));
                        document.append("capitalPais", paisJson.get("capital"));
                        document.append("region", paisJson.get("region"));
                        document.append("poblacion", paisJson.get("population"));
                        List latlng = (List) paisJson.get("latlng");
                        document.append("latitud", (double) latlng.get(0));
                        document.append("longitud", (double) latlng.get(1));
                        document.append("superficie", paisJson.get("area"));
                        
                        Document found = collection.find(eq("codigoPais", codigo)).first();
                        
                        if (found != null) {
                            Bson updateOperation = new Document("$set", document);
                            collection.updateOne(found, updateOperation);
                            System.out.println("Registro actualizado!");
                        } else {
                            collection.insertOne(document);
                            System.out.println("Registro agregado!");
                        }
                        
                        document.clear();
                        
                    }
                } else {
                    continue;
                }
                in.close();
            } catch (Exception e) {
                System.out.println("No existe un pais con el código: " + codigo);
            }
        }
        
        System.gc();
        
        System.out.println("\nPaíses con región Americas: ");
        imprimeAmericas(collection);
        
        System.out.println("\nPaises con región Americas y población mayor a 100000000");
        regionPoblacion(collection);
        
        System.out.println("\nPaises con región distinta a Africa");
        neAfrica(collection);
        
        System.out.println("\nBuscando Egypt para actualizar...");
        updateEgypt(collection);
        
        System.out.println("\nBuscando código 258 para eliminarlo de la colección...");
        delete258(collection);
        
        System.out.println("\nPoblación mayor que 50000000 y menor que 150000000");
        between(collection);
        
        System.out.println("\nPaises ordenado por nombre (Ascendente): ");
        sortNombre(collection);

        System.out.println("\nConvirtiendo a codigoPais en INDEX");
        try {
            collection.createIndex(Indexes.ascending("codigoPais"));

            System.out.println("Index creado para codigoPais");
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

    }

    public static void imprimeAmericas(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("region", "Americas");

        MongoCursor<Document> cursor = collection.find(query).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }

    public static void regionPoblacion(MongoCollection collection) {

        BasicDBObject criteria = new BasicDBObject();
        criteria.put("region", "Americas");
        criteria.put("poblacion", new Document("$gt", 50000000));

        MongoCursor<Document> cursor = collection.find(criteria).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

    }

    public static void neAfrica(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.append("region", new BasicDBObject("$ne", "Africa"));

        MongoCursor<Document> cursor = collection.find(query).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }
    }

    public static void updateEgypt(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("nombrePais", "Egypt");

        Document found = (Document) collection.find(new Document("nombrePais", "Egypt")).first();

        if (found != null) {

            Bson update = new Document("nombrePais", "Egipto").append("poblacion", 95000000);
            Bson updateEgypt = new Document("$set", update);
            collection.updateOne(found, updateEgypt);
            System.out.println("Egypt actualizado");

        }

    }

    public static void delete258(MongoCollection collection) {

        BasicDBObject query = new BasicDBObject();
        query.put("codigoPais", 258);
        DeleteResult result = collection.deleteOne(query);
        System.out.println("Resultado de la operación: " + result.getDeletedCount());

    }

    public static void between(MongoCollection collection) {

        FindIterable<Document> iterable = collection.find(
                new Document("poblacion", new Document("$gt", 50000000).append("$lt", 150000000)));

        MongoCursor<Document> cursor = iterable.iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }

    public static void sortNombre(MongoCollection collection) {

        MongoCursor<Document> cursor = collection.find().sort(new BasicDBObject("nombrePais", 1)).iterator();

        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }

    }
    
}
