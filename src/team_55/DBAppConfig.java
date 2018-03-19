package team_55;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class DBAppConfig {
    
    int mMaximumRowsCountinPage = 0;
    int mBRINSize = 0;
    
    public DBAppConfig() throws IOException {

        InputStream inputStream = null;

        try {
            Properties prop = new Properties();
            String propFileName = "DBApp.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            mMaximumRowsCountinPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
            mBRINSize = Integer.parseInt(prop.getProperty("BRINSize"));

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }

    }

    public int getmMaximumRowsCountinPage() {
        return mMaximumRowsCountinPage;
    }

    public int getmBRINSize() {
        return mBRINSize;
    }
    

}
