package zx.soft.zookeeper.book;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zookeeper.book.Client;
import zx.soft.zookeeper.book.Master;
import zx.soft.zookeeper.book.Worker;
import zx.soft.zookeeper.book.Client.TaskObject;
import zx.soft.zookeeper.book.Master.MasterStates;

public class TestTaskAssignment extends BaseTestCase {

	private static final Logger logger = LoggerFactory.getLogger(TestTaskAssignment.class);

	@Test(timeout = 50000)
	public void taskAssignmentSequential() throws Exception {
		logger.info("Starting master - Sequential");
		Master master = new Master("localhost:" + port);
		master.startZK();

		while (!master.isConnected()) {
			Thread.sleep(500);
		}

		master.bootstrap();
		master.runForMaster();

		while (master.getState() == MasterStates.RUNNING) {
			Thread.sleep(100);
		}

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
		worker1.getTasks();

		worker2.register();
		worker2.getTasks();

		worker3.register();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && worker1.isConnected() && worker2.isConnected() && worker3.isConnected()) {
			Thread.sleep(100);
		}

		TaskObject task = null;
		for (int i = 1; i < 200; i++) {
			task = new TaskObject();
			client.submitTask("Sample task", task);
			task.waitUntilDone();
			Assert.assertTrue("Task not done", task.isDone());
		}

		master.close();
		worker1.close();
		worker2.close();
		worker3.close();
		client.close();
	}

	@Test(timeout = 50000)
	public void taskAssignmentParallel() throws Exception {
		logger.info("Starting master - Parallel");
		Master master = new Master("localhost:" + port);
		master.startZK();

		while (!master.isConnected()) {
			Thread.sleep(500);
		}

		master.bootstrap();
		master.runForMaster();

		while (master.getState() == MasterStates.RUNNING) {
			Thread.sleep(100);
		}

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
		worker1.getTasks();

		worker2.register();
		worker2.getTasks();

		worker3.register();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && worker1.isConnected() && worker2.isConnected() && worker3.isConnected()) {
			Thread.sleep(100);
		}

		ArrayList<TaskObject> tasks = new ArrayList<TaskObject>();
		for (int i = 1; i < 200; i++) {
			TaskObject task = new TaskObject();
			client.submitTask("Sample task", task);
		}

		for (TaskObject task : tasks) {
			task.waitUntilDone();
			Assert.assertTrue("Task not done", task.isDone());
		}

		master.close();
		worker1.close();
		worker2.close();
		worker3.close();
		client.close();
	}

	@Test(timeout = 50000)
	public void taskZooKeeperCrash() throws Exception {
		logger.info("Starting master - ZooKeeper Crash");
		Master master = new Master("localhost:" + port);
		master.startZK();

		while (!master.isConnected()) {
			Thread.sleep(500);
		}

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
		worker1.getTasks();

		worker2.register();
		worker2.getTasks();

		worker3.register();
		worker3.getTasks();

		logger.info("Starting client");
		Client client = new Client("localhost:" + port);
		client.startZK();

		while (!client.isConnected() && worker1.isConnected() && worker2.isConnected() && worker3.isConnected()) {
			Thread.sleep(100);
		}

		TaskObject task = null;
		for (int i = 1; i < 200; i++) {
			task = new TaskObject();
			client.submitTask("Sample task", task);
		}

		/*
		 * Restart ZooKeeper server
		 */
		restartServer();
		logger.info("ZooKeeper server restarted");

		/*
		 * Let's start a new master
		 */
		master.runForMaster();

		/*
		 * ... and wait until the master is up
		 */
		while (master.getState() == MasterStates.RUNNING) {
			Thread.sleep(100);
		}

		if (task != null) {
			logger.info("Task I'm waiting for: " + task.getTaskName());
			task.waitUntilDone();
		} else {
			logger.error("Task is null.");
		}

		master.close();
		worker1.close();
		worker2.close();
		worker3.close();
		client.close();
	}

}
