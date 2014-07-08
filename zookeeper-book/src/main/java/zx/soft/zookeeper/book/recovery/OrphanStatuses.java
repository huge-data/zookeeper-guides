package zx.soft.zookeeper.book.recovery;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clients are supposed to delete status znodes. If they crash before
 * cleaning up such status znodes, they will hang there forever. This 
 * class cleans up such znodes.
 * 
 * @author wanggang
 *
 */
public class OrphanStatuses {

	private static final Logger logger = LoggerFactory.getLogger(OrphanStatuses.class);

	private List<String> tasks;
	private List<String> statuses;
	private final ZooKeeper zk;

	OrphanStatuses(ZooKeeper zk) {
		this.zk = zk;
	}

	public void cleanUp() {
		getTasks();
	}

	private void getTasks() {
		zk.getChildren("/tasks", false, tasksCallback, null);
	}

	ChildrenCallback tasksCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				tasks = children;
				getStatuses();
				break;
			default:
				logger.error("getChildren failed", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void getStatuses() {
		zk.getChildren("/status", false, statusCallback, null);
	}

	ChildrenCallback statusCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				statuses = children;
				processTasks();
				break;
			default:
				logger.error("getChildren failed", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void processTasks() {
		for (String s : tasks) {
			statuses.remove("status-" + s);
		}

		for (String s : statuses) {
			zk.delete("/status/" + s, -1, deleteStatusCallback, null);
		}
	}

	VoidCallback deleteStatusCallback = new VoidCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				zk.delete(path, -1, deleteStatusCallback, null);
				break;
			case OK:
				logger.info("Succesfully deleted orphan status znode: " + path);
				break;
			default:
				logger.error("getChildren failed", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	public static void main(String args[]) throws Exception {
		ZooKeeper zk = new ZooKeeper("localhost:" + args[0], 10000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				logger.info("Received event: " + event.getType());
			}
		});

		(new OrphanStatuses(zk)).cleanUp();
	}

}
