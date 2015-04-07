package eu.leads.distsum;

import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;

/**
 * @author Pierre Sutra
 */
public class ComChannelConverterFactory implements CacheEventConverterFactory {

   @Override
   public CacheEventConverter getConverter(Object[] params) {
      return new ComChannelEventConverter();
   }

   private static class ComChannelEventConverter implements CacheEventConverter, Serializable {

      @Override
      public Object convert(
            Object key, Object oldValue, Metadata oldMetadata,
            Object newValue, Metadata newMetadata,
            EventType eventType) {
         return newValue;
      }
   }

}
