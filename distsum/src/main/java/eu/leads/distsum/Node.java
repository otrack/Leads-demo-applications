package eu.leads.distsum;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;

/**
 *
 * @author vagvaz
 * @author otrack

 * Created by vagvaz on 7/5/14.
 */

@ClientListener(filterFactoryName = "comchannel-factory", converterFactoryName = "comchannel-factory")
public abstract class Node {

    public static final String COORDINATOR = "COORDINATOR";
    public String id;

    public Node(String i, ComChannel channel){
        id = i;
        channel.register(id,this);
    }

    public abstract void receiveMessage(Message msg);


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @ClientCacheEntryModified
    public void onCacheModification(ClientCacheEntryCustomEvent event){
       this.receiveMessage((Message) event.getEventData());
    }

}
