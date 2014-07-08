package zx.soft.zookeeper.book;

import org.junit.Test;

import zx.soft.zookeeper.book.Master;
import zx.soft.zookeeper.book.Worker;
import zx.soft.zookeeper.book.Master.MasterStates;

public class TestWorkerChanges extends BaseTestCase {

	@Test(timeout = 50000)
	public void addWorker() throws Exception {
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

		master.close();

		Worker worker = new Worker("localhost:" + port);
		worker.startZK();

		while (!worker.isConnected()) {
			Thread.sleep(100);
		}

		/*
		 * bootstrap() create some necessary znodes.
		 */
		worker.bootstrap();

		/*
		 * Registers this worker so that the leader knows that
		 * it is here.
		 */
		worker.register();

		while (master.getWorkersSize() == 0) {
			Thread.sleep(100);
		}

		worker.close();
	}

}
