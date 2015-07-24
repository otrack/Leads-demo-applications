package eu.leads.application;

import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.SingleFieldValueFilter;
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

         // configuration
         DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration,String.class,WebPage.class);
         store.createSchema();

         // Compute the average degree of the top 1000 first pages in http://www.roadrunnersports.com/ (whatever the version is)
         // We use the API of Apache Gora and its support in ISPN.
         Query query = store.newQuery();
         query.setLimit(1000);
         query.setFields("score", "url", "fetchTime");
         query.setSortingOrder(false);
         query.setSortingField("score");
         SingleFieldValueFilter fieldValueFilter = new SingleFieldValueFilter();
         fieldValueFilter.setFieldName("url");
         fieldValueFilter.setFilterOp(FilterOp.LIKE);
         fieldValueFilter.setOperands(new String[] { "%roadrunnersports%" });
         query.setFilter(fieldValueFilter);
         Result<String,WebPage>  result = query.execute();
         int averageInlinks = 0;
         int count = 0;
         while(result.next()){
            WebPage page = result.get();
            System.out.println(page.getUrl()+", "+page.getScore()+", "+page.getFetchTime());
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
