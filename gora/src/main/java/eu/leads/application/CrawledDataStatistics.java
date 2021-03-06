package eu.leads.application;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.infinispan.query.InfinispanQuery;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.FilterUtils;
import org.apache.nutch.util.NutchConfiguration;

import java.io.BufferedReader;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pierre Sutra
 */
public class CrawledDataStatistics {

   static Map<String, AtomicInteger> urls = new ConcurrentHashMap<>();

   public static void main(String[] args){

      Configuration configuration = NutchConfiguration.create();
      try {

         Result<String,WebPage> result;
         Query query;

         // 0 - Configuration
         final DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration, String.class, WebPage.class);
         store.createSchema();

         // 1 - Print the total number of content available in the store
         // We use the API of Apache Gora and its support in ISPN.
         query = store.newQuery();
         query.setFields("key");
         query.setFilter(FilterUtils.getFetchedFilter());
         query.setLimit(1);
         query.execute();
         int total = ((InfinispanQuery)query).getResultSize();
         System.out.println("Total amount of (expected) fetched web pages: "+total);

         // 2 - Retrieve all data keys in the remote store using pagination and per location/server splitting
         System.out.println("Loading keys [1/2]");
         Queue<String> keys = new ConcurrentLinkedQueue<>();
         Map<InetSocketAddress,List<Query>> queries = new HashMap<>();
         ExecutorService service = Executors.newCachedThreadPool();
         Collection<Future> futures = new ArrayList<>();

         query = store.newQuery();
         query.setFields("key");
         query.setFilter(FilterUtils.getFetchedFilter());
         query.setLimit(1);
         List<PartitionQuery> splits = ((InfinispanQuery) query).split();
         for (int s = 0; s < splits.size(); s++) {
            Query locationQuery = splits.get(s);
            InetSocketAddress location = ((InfinispanQuery) locationQuery).getLocation();
            System.out.println("Fetching from "+location);
            queries.put(location, new ArrayList<Query>());
            locationQuery.execute();
            int limit = ((InfinispanQuery) locationQuery).getResultSize();
            int blockSize = 1000;
            for (int i = 0; i < limit; i += blockSize) {
               InfinispanQuery partialQuery = (InfinispanQuery) store.newQuery();
               partialQuery.setFields("key, url");
               partialQuery.setOffset(i);
               partialQuery.setLimit(blockSize);
               partialQuery.setFilter(FilterUtils.getFetchedFilter());
               Query q = (Query) partialQuery.split().get(s);
               queries.get(location).add(q);
            }
         }

         // 3 - Lead all urls, and print some statistics
         System.out.print("Loading keys [2/2]");
         for (InetSocketAddress location : queries.keySet()) {
            futures.add(
                  service.submit(
                        new KeyLoaderCallable(queries.get(location), keys)));
         }

         for (Future future : futures)
            future.get();

         System.out.println("");
         System.out.println("Statistics");
         System.out.println("#pages (w. content): " + keys.size());
         int oneVersion = 0, fiveVersions= 0;
         Set<String> heavyVersionedPages = new HashSet<>();
         for(CharSequence key : urls.keySet()) {
            if (urls.get(key).get() > 1)
               oneVersion++;
            if (urls.get(key).get() > 5) {
               fiveVersions++;
               heavyVersionedPages.add(key.toString());
            }
         }
         System.out.println("#pages (>1 versions):" + oneVersion);
         System.out.println("#pages (>5 versions):" + fiveVersions);

         // 4 - grab some URL with lots of versions, print differences between the versions
         String heavyVersionedPage = heavyVersionedPages.iterator().next().toString();
         for(String versionedPage : heavyVersionedPages) {
            if(versionedPage.contains("edu")){
               heavyVersionedPage = versionedPage.toString();
               break;
            }
         }
         System.out.println("Computing diff for "+heavyVersionedPage);
         query = store.newQuery();
         query.setLimit(100);
         query.setFields("score", "url", "fetchTime", "content", "inlinks");
         query.setSortingOrder(false);
         query.setSortingField("score");
         SingleFieldValueFilter fieldValueFilter = new SingleFieldValueFilter();
         fieldValueFilter.setFieldName("url");
         fieldValueFilter.setFilterOp(FilterOp.LIKE);
         fieldValueFilter.setOperands(new String[] { heavyVersionedPage });
         query.setFilter(fieldValueFilter);
         result = query.execute();

         List<String> previousText = null;
         WebPage previousPage = null;
         while(result.next()){
            WebPage currentPage = result.get();
            System.out.println(currentPage.getUrl()+", "+currentPage.getFetchTime());
            List<String> currentText = new ArrayList<>();
            if (currentPage.getContent()==null) continue;
            BufferedReader reader =
                  new BufferedReader(
                        new StringReader(
                              new String(currentPage.getContent().array())));
            String line;
            while( (line=reader.readLine()) != null ){currentText.add(line);}
            if (previousText!=null) {
               System.out.println(
                     "****** version"
                           + previousPage.getFetchTime()
                           + " -- "
                           + currentPage.getFetchTime());
               Patch patch = DiffUtils.diff(previousText, currentText);
               for(Delta delta : patch.getDeltas()) {
                  System.out.println("+ "+delta.getOriginal().toString());
                  System.out.println("- " + delta.getRevised().toString());
               }
            }
            previousText = currentText;
            previousPage = currentPage;
         }

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
               if (!urls.containsKey(result.get().getUrl()))
                     urls.putIfAbsent(result.get().getUrl(), new AtomicInteger(0));
               urls.get(result.get().getUrl()).incrementAndGet();
            }
            System.out.print(".");
         }
         return null;
      }

   }

}

