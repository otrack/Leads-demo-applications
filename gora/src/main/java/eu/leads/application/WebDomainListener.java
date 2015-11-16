package eu.leads.application;

import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.FilterUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.fusesource.jansi.AnsiConsole;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * @author Pierre Sutra
 */
public class WebDomainListener {

   //private static final String servers="87.190.239.18:11234|80.156.73.113:11234|80.156.222.28:11234";
   private static final String servers="87.190.239.18:11234";

   public static void main(String[] args){

      EnsembleCacheManager manager = new EnsembleCacheManager(servers);
      EnsembleCache<UUID,WebPage> webpages = manager.getCache(
            "WebPage",
            new ArrayList<>(manager.sites()),
            EnsembleCacheManager.Consistency.DIST);

      manager.getLocalSite().getManager().
            getCache(RemoteCacheManager.DEFAULT_CACHE_NAME).addClientListener(new DomainCounterListener());

      for(String url: retrieveFetchedUrls()){
         manager.getLocalSite().getManager().
               getCache(RemoteCacheManager.DEFAULT_CACHE_NAME).put(url, "");
      }

   }

   public static String getDomainName(String url) {
      try {
         URI uri = new URI(url);
         String domain = uri.getHost();
         return domain.startsWith("www.") ? domain.substring(4) : domain;
      }catch (Exception e){
         // fail silently
      }
      return null;
   }

   public static Collection<String> retrieveFetchedUrls(){

      Configuration configuration = NutchConfiguration.create();
      configuration.set(
            "gora.datastore.connectionstring",
            "87.190.239.18:11234");

      Collection<String> urls = new ArrayList<>();
      try {

         Query query;
         final DataStore<String,WebPage> store = StorageUtils.createStore(
               configuration, String.class, WebPage.class);
         store.createSchema();

         query = store.newQuery();
         query.setFields("url");
         query.setLimit(100);
         query.setFilter(FilterUtils.getFetchedFilter());
         Result<String, WebPage> result = query.execute();
         while (result.next()) {
            urls.add(result.get().getUrl());
         }

      } catch(Exception e) {
         e.printStackTrace();
      }

      return urls;

   }

   @ClientListener
   public static class DomainCounterListener {

      private Map<String,Integer> domains = new HashMap<>();
      private Map<String, Integer> topk = new HashMap<>();
      private String lastDomain;
      private Integer lastCount;

      public DomainCounterListener(){
         AnsiConsole.systemInstall();
      }

      @Deprecated
      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      public void onCacheModification(ClientEvent event) throws URISyntaxException {
         String url;
         if (event.getType().equals(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED)){
            url = (String) ((ClientCacheEntryCreatedEvent)event).getKey();
         } else if (event.getType().equals(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED)){
            url = (String) ((ClientCacheEntryModifiedEvent)event).getKey();
         } else {
            throw new IllegalArgumentException("Invalid event :"+event.getType());
         }

         String domain = getDomainName(url);
         if (domain!=null) {
            countDoamin(domain);
            updateTopK(domain);
            printTopK();
         }
      }

      private void printTopK() {
         System.out.println(topk);
      }

      private void updateTopK(String domain) {

         assert !domains.containsKey(domain) || domains.get(domain) >= 0;
         assert topk.size()<=5;

         if (topk.size() < 5) {
            topk.put(domain, domains.get(domain));
         } else if (domains.containsKey(domain) && domains.get(domain) > lastCount) {
            assert topk.containsKey(lastDomain);
            topk.remove(lastDomain);
            int domainCount = (domains.containsKey(domain) ? domains.get(domain) : 0);
            topk.put(domain, domainCount);
         }

         // update lastCount and lastDomain
         lastCount = Integer.MAX_VALUE;
         for (String d : topk.keySet()) {
            if (topk.get(d).intValue() <= lastCount) {
               lastDomain = d;
               lastCount = topk.get(d);
            }
         }
         assert topk.containsKey(lastDomain) && lastCount!=null;

      }

      private synchronized void countDoamin(String domain){
         if (!domains.containsKey(domain)) {
            domains.put(domain,0);
         }
         domains.put(domain, domains.get(domain)+1);
         System.out.println("Received "+domain);
      }

   }

}
