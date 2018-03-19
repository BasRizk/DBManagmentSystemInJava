package team_55;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class DBAppConfig {    
    
    public int getPropValues() throws IOException {

        int maximumRowsCountinPage = 0;

        InputStream inputStream = null;

        try {
            Properties prop = new Properties();
            String propFileName = "config/DBApp.config";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            maximumRowsCountinPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return maximumRowsCountinPage;

    }

}
