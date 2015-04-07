package eu.leads.distsum;

import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

/**
 * @author Pierre Sutra
 */
public class ComChannelFilterFactory implements CacheEventFilterFactory {

   @Override
   public <K, V> CacheEventFilter<K, V> getFilter(final Object[] params) {
      return new ComChannelCacheEventFilter<K, V>(params);
   }

   private static class ComChannelCacheEventFilter<K,V> implements CacheEventFilter<K, V> , Serializable {

      private Object[] params;

      private ComChannelCacheEventFilter(Object[] p){
         params = p;
      }

      @Override
      public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata,
            EventType eventType) {
         return params[0].equals(key);
      }
   };


}
