package eu.leads.distsum;

import org.infinispan.Cache;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

/**
 *
 * @author vagvaz
 * @author otrack
 *
 * Created by vagvaz on 7/5/14.
 *
 * Communication channel just just a map of String,Node to send messages
 */
public class ComChannel {
  
  private Cache<String,Message> nodes;

  public ComChannel(Cache<String,Message> c) {
      nodes =  c;
  }

  // Add a node to the map
  public void register(final String id, Node node){
      KeyValueFilter<String,Message> filter= new KeyValueFilter<String,Message>(){
          public boolean accept(String i, Message msg, Metadata metadata){
              return id.equals(i);
          }
      };
      nodes.put(id, Message.EMPTYMSG); // to create the entry
      nodes.addListener(node, filter, null);
  }

  //Send messsage to node id
  public void sentTo(String id, Message message){
    nodes.put(id, message);
  }

  // Broadcast a message to all nodes, but coordinator
  // The coordinator takes as a result the replies of the nodes
  public void broadCast(Message message){
    for(String node: nodes.keySet()){
      if(!node.equals(Node.COORDINATOR)){
          nodes.put(node,message);
      }
    }
  }

}
