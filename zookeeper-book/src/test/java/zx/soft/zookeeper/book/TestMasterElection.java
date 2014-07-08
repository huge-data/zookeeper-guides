package zx.soft.zookeeper.book;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zookeeper.book.Master;
import zx.soft.zookeeper.book.Master.MasterStates;

public class TestMasterElection extends BaseTestCase {

	private static final Logger logger = LoggerFactory.getLogger(TestMasterElection.class);

	@Test(timeout = 50000)
	public void electMaster() throws Exception {
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

		Assert.assertTrue("Master not elected.", master.getState() == MasterStates.ELECTED);
		master.close();
	}

	@Test(timeout = 50000)
	public void reElectMaster() throws Exception {
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

		Master bmaster = new Master("localhost:" + port);
		bmaster.startZK();

		while (!bmaster.isConnected()) {
			Thread.sleep(500);
		}

		bmaster.bootstrap();
		bmaster.runForMaster();

		while (bmaster.getState() == MasterStates.RUNNING) {
			Thread.sleep(100);
		}

		Assert.assertTrue("Master not elected.", bmaster.getState() == MasterStates.ELECTED);
		bmaster.close();
	}

	@Test(timeout = 50000)
	public void electSingleMaster() throws Exception {
		Master master = new Master("localhost:" + port);
		Master bmaster = new Master("localhost:" + port);
		master.startZK();
		bmaster.startZK();

		while (!master.isConnected() || !bmaster.isConnected()) {
			Thread.sleep(500);
		}

		master.bootstrap();
		bmaster.bootstrap();

		master.runForMaster();
		bmaster.runForMaster();

		while ((master.getState() == MasterStates.RUNNING) || (bmaster.getState() == MasterStates.RUNNING)) {
			Thread.sleep(100);
		}

		boolean singleMaster = (((master.getState() == MasterStates.ELECTED) && (bmaster.getState() != MasterStates.ELECTED)) || ((master
				.getState() != MasterStates.ELECTED) && (bmaster.getState() == MasterStates.ELECTED)));
		Assert.assertTrue("Master not elected.", singleMaster);
		master.close();
		bmaster.close();
	}

	@Test(timeout = 50000)
	public void testMasterExists() throws Exception {
		Master master = new Master("localhost:" + port);

		master.startZK();

		while (!master.isConnected()) {
			Thread.sleep(500);
		}

		master.bootstrap();
		master.masterExists();

		int attempts = 10;
		boolean elected = true;
		while ((master.getState() == MasterStates.RUNNING)) {
			Thread.sleep(200);
			if (attempts-- == 0) {
				logger.info("Breaking...");
				elected = false;
				break;
			}
		}

		Assert.assertTrue("Master not elected.", elected);
		master.close();
	}

}
