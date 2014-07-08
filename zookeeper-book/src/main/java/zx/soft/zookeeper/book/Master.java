package zx.soft.zookeeper.book;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zookeeper.book.recovery.RecoveredAssignments;
import zx.soft.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;

/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers. 
 * 
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * 
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * 
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED. 
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 *   
 * Because workers may crash, this master also needs to be able to reassign
 * tasks. When it watches for changes in the list of workers, it also 
 * receives a notification when a znode representing a worker is gone, so 
 * it is able to reassign its tasks.
 * 
 * A primary may crash too. In the case a primary crashes, the next primary
 * that takes over the role needs to make sure that it assigns and reassigns
 * tasks that the previous primary hasn't had time to process.
 * 
 * @author wanggang
 *
 */
public class Master implements Watcher, Closeable {

	private static final Logger logger = LoggerFactory.getLogger(Master.class);

	/*
	 * A master process can be either running for
	 * primary master, elected primary master, or
	 * not elected, in which case it is a backup
	 * master.  
	 */
	enum MasterStates {
		RUNNING, ELECTED, NOTELECTED
	};

	private volatile MasterStates state = MasterStates.RUNNING;

	MasterStates getState() {
		return state;
	}

	private final Random random = new Random(this.hashCode());
	private ZooKeeper zk;
	private final String hostPort;
	private final String serverId = Integer.toHexString(random.nextInt());
	private volatile boolean connected = false;
	private volatile boolean expired = false;

	protected ChildrenCache tasksCache;
	protected ChildrenCache workersCache;

	/**
	 * Creates a new master instance.
	 * 
	 * @param hostPort
	 */
	Master(String hostPort) {
		this.hostPort = hostPort;
	}

	/**
	 * Creates a new ZooKeeper session.
	 * 
	 * @throws IOException
	 */
	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	/**
	 * Closes the ZooKeeper session.
	 *
	 * @throws IOException
	 */
	void stopZK() throws InterruptedException, IOException {
		zk.close();
	}

	/**
	 * This method implements the process method of the
	 * Watcher interface. We use it to deal with the
	 * different states of a session. 
	 * 
	 * @param e new session event to be processed
	 */
	@Override
	public void process(WatchedEvent e) {
		logger.info("Processing event: " + e.toString());
		if (e.getType() == Event.EventType.None) {
			switch (e.getState()) {
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				logger.error("Session expiration");
			default:
				break;
			}
		}
	}

	/**
	 * This method creates some parent znodes we need for this example.
	 * In the case the master is restarted, then this method does not
	 * need to be executed a second time.
	 */
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}

	void createParent(String path, byte[] data) {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
	}

	StringCallback createParentCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				/*
				 * Try again. Note that registering again is not a problem.
				 * If the znode has already been created, then we get a 
				 * NODEEXISTS event back.
				 */
				createParent(path, (byte[]) ctx);

				break;
			case OK:
				logger.info("Parent created");

				break;
			case NODEEXISTS:
				logger.warn("Parent already registered: " + path);

				break;
			default:
				logger.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/**
	 * Check if this client is connected.
	 * 
	 * @return boolean ZooKeeper client is connected
	 */
	boolean isConnected() {
		return connected;
	}

	/**
	 * Check if the ZooKeeper session has expired.
	 * 
	 * @return boolean ZooKeeper session has expired
	 */
	boolean isExpired() {
		return expired;
	}

	/*
	 **************************************
	 **************************************
	 * Methods related to master election.*
	 **************************************
	 **************************************
	 */

	/*
	 * The story in this callback implementation is the following.
	 * We tried to create the master lock znode. If it suceeds, then
	 * great, it takes leadership. However, there are a couple of
	 * exceptional situations we need to take care of. 
	 * 
	 * First, we could get a connection loss event before getting
	 * an answer so we are left wondering if the operation went through.
	 * To check, we try to read the /master znode. If it is there, then
	 * we check if this master is the primary. If not, we run for master
	 * again. 
	 * 
	 *  The second case is if we find that the node is already there.
	 *  In this case, we call exists to set a watch on the znode.
	 */
	StringCallback masterCreateCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();

				break;
			case OK:
				state = MasterStates.ELECTED;
				takeLeadership();

				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();

				break;
			default:
				state = MasterStates.NOTELECTED;
				logger.error("Something went wrong when running for master.",
						KeeperException.create(Code.get(rc), path));
			}
			logger.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
		}
	};

	void masterExists() {
		zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
	}

	StatCallback masterExistsCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();

				break;
			case OK:
				break;
			case NONODE:
				state = MasterStates.RUNNING;
				runForMaster();
				logger.info("It sounds like the previous master is gone, " + "so let's run for master again.");

				break;
			default:
				checkMaster();
				break;
			}
		}
	};

	Watcher masterExistsWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeDeleted) {
				assert "/master".equals(e.getPath());

				runForMaster();
			}
		}
	};

	void takeLeadership() {
		logger.info("Going for list of workers");
		getWorkers();

		(new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
			@Override
			public void recoveryComplete(int rc, List<String> tasks) {
				if (rc == RecoveryCallback.FAILED) {
					logger.error("Recovery of assigned tasks failed.");
				} else {
					logger.info("Assigning recovered tasks");
					getTasks();
				}
			}
		});
	}

	/*
	 * Run for master. To run for master, we try to create the /master znode,
	 * with masteCreateCallback being the callback implementation. 
	 * In the case the create call succeeds, the client becomes the master.
	 * If it receives a CONNECTIONLOSS event, then it needs to check if the 
	 * znode has been created. In the case the znode exists, it needs to check
	 * which server is the master.
	 */

	/**
	 * Tries to create a /master lock znode to acquire leadership.
	 */
	public void runForMaster() {
		logger.info("Running for master");
		zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
	}

	DataCallback masterCheckCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();

				break;
			case NONODE:
				runForMaster();

				break;
			case OK:
				if (serverId.equals(new String(data))) {
					state = MasterStates.ELECTED;
					takeLeadership();
				} else {
					state = MasterStates.NOTELECTED;
					masterExists();
				}

				break;
			default:
				logger.error("Error when reading data.", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void checkMaster() {
		zk.getData("/master", false, masterCheckCallback, null);
	}

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to handle changes to the list of workers.*
	 ****************************************************
	 ****************************************************
	 */

	/**
	 * This method is here for testing purposes.
	 * 
	 * @return size Size of the worker list
	 */
	public int getWorkersSize() {
		if (workersCache == null) {
			return 0;
		} else {
			return workersCache.getList().size();
		}
	}

	Watcher workersChangeWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged) {
				assert "/workers".equals(e.getPath());

				getWorkers();
			}
		}
	};

	void getWorkers() {
		zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
	}

	ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				logger.info("Succesfully got a list of workers: " + children.size() + " workers");
				reassignAndSet(children);
				break;
			default:
				logger.error("getChildren failed", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/*
	 *******************
	 *******************
	 * Assigning tasks.*
	 *******************
	 *******************
	 */

	void reassignAndSet(List<String> children) {
		List<String> toProcess;

		if (workersCache == null) {
			workersCache = new ChildrenCache(children);
			toProcess = null;
		} else {
			logger.info("Removing and setting");
			toProcess = workersCache.removedAndSet(children);
		}

		if (toProcess != null) {
			for (String worker : toProcess) {
				getAbsentWorkerTasks(worker);
			}
		}
	}

	void getAbsentWorkerTasks(String worker) {
		zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
	}

	ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getAbsentWorkerTasks(path);

				break;
			case OK:
				logger.info("Succesfully got a list of assignments: " + children.size() + " tasks");

				/*
				 * Reassign the tasks of the absent worker.  
				 */

				for (String task : children) {
					getDataReassign(path + "/" + task, task);
				}
				break;
			default:
				logger.error("getChildren failed", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/*
	 ************************************************
	 * Recovery of tasks assigned to absent worker. * 
	 ************************************************
	 */

	/**
	 * Get reassigned task data.
	 * 
	 * @param path Path of assigned task
	 * @param task Task name excluding the path prefix
	 */
	void getDataReassign(String path, String task) {
		zk.getData(path, false, getDataReassignCallback, task);
	}

	/**
	 * Context for recreate operation.
	 *
	 */
	class RecreateTaskCtx {
		String path;
		String task;
		byte[] data;

		RecreateTaskCtx(String path, String task, byte[] data) {
			this.path = path;
			this.task = task;
			this.data = data;
		}
	}

	/**
	 * Get task data reassign callback.
	 */
	DataCallback getDataReassignCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getDataReassign(path, (String) ctx);

				break;
			case OK:
				recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

				break;
			default:
				logger.error("Something went wrong when getting data ", KeeperException.create(Code.get(rc)));
			}
		}
	};

	/**
	 * Recreate task znode in /tasks
	 * 
	 * @param ctx Recreate text context
	 */
	void recreateTask(RecreateTaskCtx ctx) {
		zk.create("/tasks/" + ctx.task, ctx.data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, ctx);
	}

	/**
	 * Recreate znode callback
	 */
	StringCallback recreateTaskCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				recreateTask((RecreateTaskCtx) ctx);

				break;
			case OK:
				deleteAssignment(((RecreateTaskCtx) ctx).path);

				break;
			case NODEEXISTS:
				logger.info("Node exists already, but if it hasn't been deleted, "
						+ "then it will eventually, so we keep trying: " + path);
				recreateTask((RecreateTaskCtx) ctx);

				break;
			default:
				logger.error("Something wwnt wrong when recreating task", KeeperException.create(Code.get(rc)));
			}
		}
	};

	/**
	 * Delete assignment of absent worker
	 * 
	 * @param path Path of znode to be deleted
	 */
	void deleteAssignment(String path) {
		zk.delete(path, -1, taskDeletionCallback, null);
	}

	VoidCallback taskDeletionCallback = new VoidCallback() {
		@Override
		public void processResult(int rc, String path, Object rtx) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				deleteAssignment(path);
				break;
			case OK:
				logger.info("Task correctly deleted: " + path);
				break;
			default:
				logger.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/*
	 ******************************************************
	 ******************************************************
	 * Methods for receiving new tasks and assigning them.*
	 ******************************************************
	 ******************************************************
	 */
	Watcher tasksChangeWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged) {
				assert "/tasks".equals(e.getPath());

				getTasks();
			}
		}
	};

	void getTasks() {
		zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
	}

	ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTasks();

				break;
			case OK:
				List<String> toProcess;
				if (tasksCache == null) {
					tasksCache = new ChildrenCache(children);

					toProcess = children;
				} else {
					toProcess = tasksCache.addedAndSet(children);
				}

				if (toProcess != null) {
					assignTasks(toProcess);
				}

				break;
			default:
				logger.error("getChildren failed.", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void assignTasks(List<String> tasks) {
		for (String task : tasks) {
			getTaskData(task);
		}
	}

	void getTaskData(String task) {
		zk.getData("/tasks/" + task, false, taskDataCallback, task);
	}

	DataCallback taskDataCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getTaskData((String) ctx);

				break;
			case OK:
				/*
				 * Choose worker at random.
				 */
				List<String> list = workersCache.getList();
				String designatedWorker = list.get(random.nextInt(list.size()));

				/*
				 * Assign task to randomly chosen worker.
				 */
				String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
				logger.info("Assignment path: " + assignmentPath);
				createAssignment(assignmentPath, data);

				break;
			default:
				logger.error("Error when trying to get task data.", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void createAssignment(String path, byte[] data) {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
	}

	StringCallback assignTaskCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createAssignment(path, (byte[]) ctx);

				break;
			case OK:
				logger.info("Task assigned correctly: " + name);
				deleteTask(name.substring(name.lastIndexOf("/") + 1));

				break;
			case NODEEXISTS:
				logger.warn("Task already assigned");

				break;
			default:
				logger.error("Error when trying to assign task.", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/**
	 * Once assigned, we delete the task from /tasks
	 */
	void deleteTask(String name) {
		zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
	}

	VoidCallback taskDeleteCallback = new VoidCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				deleteTask(path);

				break;
			case OK:
				logger.info("Successfully deleted " + path);

				break;
			case NONODE:
				logger.info("Task has been deleted already");

				break;
			default:
				logger.error("Something went wrong here, " + KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/**
	 * Closes the ZooKeeper session. 
	 * 
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		if (zk != null) {
			try {
				zk.close();
			} catch (InterruptedException e) {
				logger.warn("Interrupted while closing ZooKeeper session.", e);
			}
		}
	}

	/**
	 * Main method providing an example of how to run the master.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {

		Master master = new Master(args[0]);
		master.startZK();

		while (!master.isConnected()) {
			Thread.sleep(100);
		}
		/*
		 * bootstrap() creates some necessary znodes.
		 */
		master.bootstrap();

		/*
		 * now runs for master.
		 */
		master.runForMaster();

		while (!master.isExpired()) {
			Thread.sleep(1000);
		}

		master.stopZK();
		master.close();
	}
}
