package eu.leads.application;

import org.apache.gora.infinispan.query.InfinispanQuery;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author Pierre Sutra
 * @since 1.0
 */
public class SimpleApplication {

   public static void main(String[] args){

      Configuration configuration = NutchConfiguration.create();
      try {

         Result<String,WebPage> result;
         Query query;

         // 0 - Configuration
         final DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration, String.class, WebPage.class);
         store.createSchema();

         // 1 - Print the total number of pages available in the store
         // We use the API of Apache Gora and its support in ISPN.
         query = store.newQuery();
         query.setFields("key");
         query.setLimit(1);
         query.execute();
         int total = ((InfinispanQuery)query).getResultSize();
         System.out.println("Total amount of (expected) web pages: "+total);

         // 2 - Retrieve the top 100 first pages from http://www.roadrunnersports.com/ (whatever the version is)
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

         // 3 - Compare the various versions of roadrunnersports.com/rrs/womensshoes/womensnewshoes/
//         query = store.newQuery();
//         query.setSortingOrder(false);
//         query.setSortingField("fetchTime");
//         query.setLimit(100);
//         fieldValueFilter = new SingleFieldValueFilter();
//         fieldValueFilter.setFieldName("url");
//         fieldValueFilter.setFilterOp(FilterOp.LIKE);
//         fieldValueFilter.setOperands(new String[] { "%roadrunnersports.com/rrs/womensshoes/womensnewshoes%" });
//         query.setFilter(fieldValueFilter);
//         result = query.execute();
//
//         List<String> previousText = null;
//         WebPage previousPage = null;
//         while(result.next()){
//            WebPage currentPage = result.get();
//            System.out.println(currentPage.getUrl()+", "+currentPage.getFetchTime());
//            List<String> currentText = new ArrayList<>();
//            if (currentPage.getContent()==null) continue;
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
//               Patch patch = DiffUtils.diff(previousText, currentText);
//               for(Delta delta : patch.getDeltas()) {
//                  System.out.println("+ "+delta.getOriginal().toString());
//                  System.out.println("- " + delta.getRevised().toString());
//               }
//            }
//            previousText = currentText;
//            previousPage = currentPage;
//         }

         // 4 - Retrieve all data in the remote store using pagination and per location/server splitting
         System.out.print("Loading keys ");
         Queue<String> keys = new ConcurrentLinkedQueue<>();
         Map<InetSocketAddress,List<Query>> queries = new HashMap<>();
         ExecutorService service = Executors.newCachedThreadPool();
         Collection<Future> futures = new ArrayList<>();

         query = store.newQuery();
         query.setFields("key");
         query.setLimit(1);
         List<PartitionQuery> splits = ((InfinispanQuery) query).split();
         for (int s = 0; s < splits.size(); s++) {
            Query locationQuery = splits.get(s);
            InetSocketAddress location = ((InfinispanQuery) locationQuery).getLocation();
            queries.put(location, new ArrayList<Query>());
            locationQuery.execute();
            int limit = ((InfinispanQuery) locationQuery).getResultSize();
            int blockSize = 1000;
            for (int i = 0; i < limit; i += blockSize) {
               InfinispanQuery partialQuery = (InfinispanQuery) store.newQuery();
               partialQuery.setFields("key");
               partialQuery.setOffset(i);
               if (i + blockSize < limit)
                  query.setLimit(blockSize);
               Query q = (Query) partialQuery.split().get(s);
               queries.get(location).add(q);
            }
         }

         for (InetSocketAddress location : queries.keySet()) {
            futures.add(
                  service.submit(
                        new KeyLoaderCallable(queries.get(location), keys)));
         }

         for (Future future : futures)
            future.get();

         System.out.println(" done " + keys.size());

      } catch(Exception e) {
         e.printStackTrace();
      }

      System.exit(0);

   }

   private static class KeyLoaderCallable implements Callable<Void> {

      List<Query> queries;
      Queue<String> keys;

      public KeyLoaderCallable(List<Query> queries, Queue<String> keys) {
         this.queries = queries;
         this.keys = keys;
      }

      @Override
      public Void call() throws Exception {
         for (Query q : queries) {
            Result<String, WebPage> result = q.execute();
            while (result.next()) {
               keys.add(result.getKey());
            }
            System.out.print(".");
         }
         return null;
      }

   }

}

