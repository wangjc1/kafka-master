package my;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
 
public class ZKSessionExpiredTest {
	
	private static ZooKeeper zookeeper;
	
	private static CountDownLatch latch;
	
	public static ZooKeeper getZooKeeper() {
		  if (zookeeper == null) {
		    synchronized (ZKSessionExpiredTest.class) {
		      if (zookeeper == null) {
		        latch = new CountDownLatch(1);
		        zookeeper = buildClient();//如果失败，下次还有成功的机会
		        try {
		           latch.await(30, TimeUnit.SECONDS);
		        } catch (InterruptedException e) {
		           e.printStackTrace();
		        } finally {
		          latch = null;
		        }
		      }
		    }
		  }
		  return zookeeper;
	}
 
	private static ZooKeeper buildClient() {
		try {
			return new ZooKeeper("localhost:2181", 10000, new SessionWatcher());
		} catch (IOException e) {
			throw new RuntimeException("init zookeeper fail.", e);
		}
	}
	
	static class SessionWatcher implements Watcher {
 
	    public void process(WatchedEvent event) {
	        if (event.getState() == KeeperState.SyncConnected) {
	            if (latch != null) {
	                latch.countDown();
	            }
	        } else if (event.getState() == KeeperState.Expired) {
	            System.out.println("[SUC-CORE] session expired. now rebuilding");
	            close();
	            getZooKeeper();
	        }
	    }
 
		private void close() {
			 System.out.println("[SUC-CORE] close");
			    if (zookeeper != null) {
			        try {
			            zookeeper.close();
			            zookeeper = null;
			        } catch (InterruptedException e) {
			           
			        }
			    }
		}
	}
	
	public static void main(String[] args) throws Exception {
		  ZooKeeper zk = ZKSessionExpiredTest.getZooKeeper();
		  long sessionId = zk.getSessionId();
		  
		  // close the old connection
		  new ZooKeeper("localhost:2181", 30000, null, sessionId, zk.getSessionPasswd()).close();
		  Thread.sleep(60000L);
 
		  // rebuild a new session
		  long newSessionid = ZKSessionExpiredTest.getZooKeeper().getSessionId();
 
		  // check the new session
		  String status = newSessionid != sessionId ? "OK" : "FAIL";
		  System.out.println(sessionId+"--->"+newSessionid+ ", status: " +status);
 
		  // close the client
		  ZKSessionExpiredTest.getZooKeeper().close();
		  Thread.currentThread().join();
	}
 
}