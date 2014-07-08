package zx.soft.zookeeper.book;

import java.util.ArrayList;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zookeeper.book.Client;
import zx.soft.zookeeper.book.Worker;
import zx.soft.zookeeper.book.Client.TaskObject;
import zx.soft.zookeeper.book.curator.CuratorMasterLatch;
import zx.soft.zookeeper.book.curator.CuratorMasterSelector;

public class TestCuratorMaster extends BaseTestCase {

	private static final Logger logger = LoggerFactory.getLogger(TestTaskAssignment.class);

	@Test(timeout = 60000)
	public void testTaskAssignment() throws Exception {
		logger.info("Starting master (taskAssignment)");
		CuratorMasterSelector master = new CuratorMasterSelector("M1", "localhost:" + port,
				new ExponentialBackoffRetry(1000, 5));
		master.startZK();

		master.bootstrap();
		master.runForMaster();

		logger.info("Going to wait for leadership");
		master.awaitLeadership();

		logger.info("Starting worker");
		Worker worker1 = new Worker("localhost:" + port);
		Worker worker2 = new Worker("localhost:" + port);
		Worker worker3 = new Worker("localhost:" + port);

		worker1.startZK();
		worker2.startZK();
		worker3.startZK();

		while (!worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		/*
		 * bootstrap() create some necessary znodes.
		 */
		worker1.bootstrap();
		worker2.bootstrap();
		worker3.bootstrap();

		/*
		 * Registers this worker so that the leader knows that
		 * it is here.
		 */
		worker1.register();
		worker2.register();
		worker3.register();

		worker1.getTasks();
		worker2.getTasks();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && !worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		TaskObject task = null;
		for (int i = 1; i < 200; i++) {
			task = new TaskObject();
			client.submitTask("Sample task taskAssignment " + i, task);
			task.waitUntilDone();
			Assert.assertTrue("Task not done", task.isDone());
		}

		worker1.close();
		worker2.close();
		worker3.close();
		master.close();
		client.close();
	}

	@Test(timeout = 60000)
	public void testZooKeeperRestart() throws Exception {
		logger.info("Starting zookeeper restart");
		CuratorMasterSelector master = new CuratorMasterSelector("M1", "localhost:" + port,
				new ExponentialBackoffRetry(1000, 5));
		master.startZK();
		master.bootstrap();

		logger.info("Starting worker");
		Worker worker1 = new Worker("localhost:" + port);
		Worker worker2 = new Worker("localhost:" + port);
		Worker worker3 = new Worker("localhost:" + port);

		worker1.startZK();
		worker2.startZK();
		worker3.startZK();

		while (!worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		/*
		 * bootstrap() create some necessary znodes.
		 */
		worker1.bootstrap();
		worker2.bootstrap();
		worker3.bootstrap();

		/*
		 * Registers this worker so that the leader knows that
		 * it is here.
		 */
		worker1.register();
		worker2.register();
		worker3.register();

		worker1.getTasks();
		worker2.getTasks();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && !worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		int numTasks = 200;
		TaskObject task = null;
		ArrayList<TaskObject> taskList = new ArrayList<TaskObject>(numTasks);
		for (int i = 0; i < numTasks; i++) {
			task = new TaskObject();
			client.submitTask("Sample task zkRestart " + i, task);
			taskList.add(task);
		}

		/*
		 * Restart ZK server
		 */
		logger.info("Restarting ZooKeeper");
		restartServer();

		/*
		 * Now try to get mastership
		 */
		master.runForMaster();

		logger.info("Going to wait for leadership");
		master.awaitLeadership();

		for (int i = 0; i < numTasks; i++) {
			taskList.get(i).waitUntilDone();
			Assert.assertTrue("Task not done", taskList.get(i).isDone());
		}

		/*
		 * Wrapping up
		 */
		worker1.close();
		worker2.close();
		worker3.close();
		master.close();
		client.close();
	}

	@Test(timeout = 30000)
	public void electSingleMaster() throws Exception {

		logger.info("Starting single master test");
		CuratorMasterSelector master = new CuratorMasterSelector("M1", "localhost:" + port,
				new ExponentialBackoffRetry(1000, 5));
		CuratorMasterSelector bmaster = new CuratorMasterSelector("M2", "localhost:" + port,
				new ExponentialBackoffRetry(1000, 5));

		logger.info("Starting ZooKeeper for M1");
		master.startZK();

		logger.info("Starting ZooKeeper for M2");
		bmaster.startZK();

		master.bootstrap();
		//bm.bootstrap();

		bmaster.runForMaster();
		master.runForMaster();

		while (!master.isLeader() && !bmaster.isLeader()) {
			logger.info("m: " + master.isLeader() + ", bm: " + bmaster.isLeader());
			Thread.sleep(100);
		}

		boolean singleMaster = ((master.isLeader() && !bmaster.isLeader()) || (!master.isLeader() && bmaster.isLeader()));
		Assert.assertTrue("Master not elected.", singleMaster);
		master.close();
		bmaster.close();
	}

	@Test(timeout = 60000)
	public void testTaskAssignmentLatch() throws Exception {
		logger.info("Starting master (taskAssignment)");
		CuratorMasterLatch master = new CuratorMasterLatch("M1", "localhost:" + port, new ExponentialBackoffRetry(1000,
				5));
		master.startZK();

		master.bootstrap();
		master.runForMaster();

		logger.info("Going to wait for leadership");
		master.awaitLeadership();

		logger.info("Starting worker");
		Worker worker1 = new Worker("localhost:" + port);
		Worker worker2 = new Worker("localhost:" + port);
		Worker worker3 = new Worker("localhost:" + port);

		worker1.startZK();
		worker2.startZK();
		worker3.startZK();

		while (!worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		/*
		 * bootstrap() create some necessary znodes.
		 */
		worker1.bootstrap();
		worker2.bootstrap();
		worker3.bootstrap();

		/*
		 * Registers this worker so that the leader knows that
		 * it is here.
		 */
		worker1.register();
		worker2.register();
		worker3.register();

		worker1.getTasks();
		worker2.getTasks();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && !worker1.isConnected() && !worker2.isConnected() && !worker3.isConnected()) {
			Thread.sleep(100);
		}

		TaskObject task = null;
		for (int i = 1; i < 200; i++) {
			task = new TaskObject();
			client.submitTask("Sample task taskAssignment " + i, task);
			task.waitUntilDone();
			Assert.assertTrue("Task not done", task.isDone());
		}

		worker1.close();
		worker2.close();
		worker3.close();
		master.close();
		client.close();
	}

}
