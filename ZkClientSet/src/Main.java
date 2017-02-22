import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
//import org.json.*;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String configPath = null;
		String configZks = null;
		
		FileInputStream in;
		try {
			in = new FileInputStream("ZkClient.cfg");
			Properties prop = new Properties();
			prop.load(in);
			in.close();
			configZks	= prop.getProperty("cfg_zks");
			if (configZks == null){
	        	return;
	        }
			configPath	= prop.getProperty("cfg_path");
			if (configPath == null){
	        	return;
	        }
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		File file = new File("node.content");  
        Long filelength = file.length();  
        byte[] fileContent = new byte[filelength.intValue()];   
        try {
			in = new FileInputStream(file);
			in.read(fileContent);  
	        in.close(); 
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}  
         
        //JSONObject jsonObj = new JSONObject(filecontent); 
		
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(configZks)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000,3))
				.build();
		client.start();  
		
	    try {    
            client.setData().forPath(configPath, fileContent);  
        } catch (Exception e) {  
            e.printStackTrace();  
        }

	}

}
