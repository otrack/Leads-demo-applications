package eu.leads.application;

import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;

/**
 *
 * @author Pierre Sutra
 * @since 1.0
 */
public class SimpleApplication {

  public static void main(String[] args){

    Configuration configuration = NutchConfiguration.create();
    try {
      DataStore<String,WebPage> store = StorageUtils.createWebStore(configuration,String.class,WebPage.class);
      store.createSchema();
      Query query = store.newQuery();
      query.setLimit(10);
      query.setOffset(100);
      query.setFields("outlinks");
      Result<String,WebPage> result = query.execute();
      int averageOutlinks = 0;
      int count = 0;
      while(result.next()){
        WebPage page = result.get();
        System.out.println("(reversed) url is: "+ result.getKey());
        averageOutlinks += page.getOutlinks().size();
        count++;
      }
      System.out.println("Average out degree: "+(float)averageOutlinks/(float)count);

    } catch (Exception e) {
      e.printStackTrace();
    }

    System.exit(0);

  }

}
