package com.event.publisher;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.event.constants.Constants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.omg.IOP.Encoding;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RequestEventPublisher {
    public static void readFileAndSendToEventHub(String filePath, double batchSize) throws IOException, ParseException {
        //reads the file
        FileReader fileReader = new FileReader(filePath);
        //creates a buffering character input stream
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        JSONParser jsonParser = new JSONParser();
        // create a producer using the namespace connection string and event hub name
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(Constants.connectionString, Constants.eventHubName)
                .buildProducerClient();
        int count = 0;
        EventDataBatch batch = null;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (count==0 || count%(int)batchSize == 0) {
                if (count != 0) {
                    producer.send(batch);
                }
                // prepare a batch of events to send to the event hub
                batch = producer.createBatch();
            }
            if (!line.equals("[")&&!line.equals("]")) {
                Object eventsObject = jsonParser.parse(line.substring(0,line.length()-1));
                String strEventJSON = ((JSONObject) eventsObject).toJSONString();
                batch.tryAdd(new EventData(strEventJSON));
                count ++;
            }
        }
        if (batch.getCount() > 0) {
            producer.send(batch);
        }
        producer.close();
        fileReader.close();
    }

    public static void main(String[] args) throws ParseException {
        double batchSize;
        String fileName;
        Properties prop = new Properties();
        InputStream input;
        try {
            input = RequestEventPublisher.class.getClassLoader().getResourceAsStream("config.properties");
            // load a properties file
            prop.load(input);
            // get the property value
            batchSize = Double.parseDouble(prop.getProperty("batchSize", "100"));
            fileName = prop.getProperty("fileName");
            if (input != null) {
                input.close();
            }
            if (fileName != null) {
                readFileAndSendToEventHub(fileName, batchSize);
            } else {
                System.out.println("Provide a filename");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
