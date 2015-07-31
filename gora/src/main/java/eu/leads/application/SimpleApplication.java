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

         // configuration
         DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration, String.class, WebPage.class);
         store.createSchema();

         Query query = store.newQuery();
         query.setFields("key","url");
         query.setLimit(100);
         Result<String,WebPage> result = query.execute();
         int counter = 0;
         while(result.next()){
            System.out.println(".");
            counter++;
         }
         System.out.println("Total amount of pages: "+counter);

//         // Retrieve the top 100 first pages from http://www.roadrunnersports.com/ (whatever the version is)
//         // We use the API of Apache Gora and its support in ISPN.
//         query = store.newQuery();
//         query.setLimit(100);
//         query.setFields("score", "url", "fetchTime", "content", "inlinks");
//         query.setSortingOrder(false);
//         query.setSortingField("score");
//         SingleFieldValueFilter fieldValueFilter = new SingleFieldValueFilter();
//         fieldValueFilter.setFieldName("url");
//         fieldValueFilter.setFilterOp(FilterOp.LIKE);
//         fieldValueFilter.setOperands(new String[] { "%roadrunnersports%" });
//         query.setFilter(fieldValueFilter);
//         result = query.execute();
//         int averageInlinks = 0;
//         int count = 0;
//         Map<String,WebPage> resultMap = new HashMap<>();
//         while(result.next()){
//            WebPage page = result.get();
//            // Print content and some usefull information
//            System.out.println(
//                  page.getUrl()
//                        +", "+page.getScore()
//                        +", "+page.getFetchTime()
//                        +", "+(page.getContent()==null ? 0 : page.getContent().array().length));
//            averageInlinks += page.getInlinks().size();
//            count++;
//
//            if (!resultMap.containsKey(page.getUrl()) || resultMap.get(page.getUrl()).getFetchTime() < page.getFetchTime())
//               resultMap.put(page.getUrl(),page);
//         }
//         System.out.println("Total amount of pages: "+count);
//         System.out.println("Average in degree: "+(float)averageInlinks/(float)count);
//
//         // Compare the various versions of  roadrunnersports.com/rrs/womensshoes/womensnewshoes/
//         query = store.newQuery();
//         query.setSortingOrder(false);
//         query.setSortingField("fetchTime");
//         query.setLimit(100);
//         fieldValueFilter = new SingleFieldValueFilter();
//         fieldValueFilter.setFieldName("url");
//         fieldValueFilter.setFilterOp(FilterOp.LIKE);
//         fieldValueFilter.setOperands(new String[] { "%rgear-full-throttle-bottle-pack-21-ounce%" });
//         query.setFilter(fieldValueFilter);
//         result = query.execute();
//
//         List<String> previousText = null;
//         WebPage previousPage = null;
//         while(result.next()){
//            WebPage currentPage = result.get();
//            System.out.println(currentPage.getUrl()+", "+currentPage.getFetchTime());
//            List<String> currentText = new ArrayList<>();
//            BufferedReader reader =
//                  new BufferedReader(
//                        new StringReader(
//                              new String(currentPage.getContent().array())));
//            String line;
//            while( (line=reader.readLine()) != null ){currentText.add(line);}
//            if (previousText!=null) {
//               System.out.println(
//                     "****** version"
//                           + previousPage.getFetchTime()
//                           + " -- "
//                           + currentPage.getFetchTime());
//               Patch patch = DiffUtils.diff(previousText,currentText);
//               for(Delta delta : patch.getDeltas()) {
//                  System.out.println("+ "+delta.getOriginal().toString());
//                  System.out.println("- " + delta.getRevised().toString());
//               }
//            }
//            previousText = currentText;
//            previousPage = currentPage;
//         }
      } catch (Exception e) {
         e.printStackTrace();
      }

      System.exit(0);

   }

}
