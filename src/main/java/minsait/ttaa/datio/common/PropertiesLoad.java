package minsait.ttaa.datio.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoad {


    /**
     * * @return Properties loaded from applicationPropertiles file
     */
    public static Properties getPropertiesFile(){
        final Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(Common.APPLICATION_PROPERTIES_PATH)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
