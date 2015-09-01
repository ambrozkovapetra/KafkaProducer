package cz.muni.fi.kafkaproducer;

import java.io.*;
import java.util.Properties;
import java.io.InputStream;

/**
 *
 * @author Petra
 */
public class PropertiesParser{

    private static final String propertiesFile = "src/main/resources/kafka.properties";

    /**
     * Parsers esper.properties file
     *
     */
    public static Properties getProperties() {
        Properties properties = new Properties();
        try {
        File file = new File(propertiesFile);
	FileInputStream fileInput = new FileInputStream(file);
	properties.load(fileInput);
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }
}
