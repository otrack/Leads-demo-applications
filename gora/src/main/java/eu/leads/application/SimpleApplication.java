package eu.leads.application;

import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.Mark;
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

         DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration,String.class,WebPage.class);

         store.createSchema();
         Query query = store.newQuery();
         query.setFields("inlinks");

         MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<>();
         filter.setFieldName(WebPage.Field.MARKERS.toString());
         filter.setFilterOp(FilterOp.LIKE);
         filter.setFilterIfMissing(false);
         filter.setMapKey(Mark.FETCH_MARK.getName());
         filter.getOperands().add("*");

         query.setFilter(filter);

         Result<String,WebPage> result = query.execute();
         int averageInlinks = 0;
         int count = 0;
         while(result.next()){
            WebPage page = result.get();
            System.out.println("(reversed) url is: "+ result.getKey());
            averageInlinks += page.getInlinks().size();
            count++;
         }
         System.out.println("Total amount of pages: "+count);
         System.out.println("Average in degree: "+(float)averageInlinks/(float)count);

      } catch (Exception e) {
         e.printStackTrace();
      }

      System.exit(0);

   }

}
