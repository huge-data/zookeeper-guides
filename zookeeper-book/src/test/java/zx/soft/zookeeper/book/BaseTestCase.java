package zx.soft.zookeeper.book;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTestCase {

	private static final Logger logger = LoggerFactory.getLogger(BaseTestCase.class);

	int port;
	File tmpDir;
	StandaloneServer zkServer;
	final static File BASETEST = new File(System.getProperty("buildTestDir", "test"));

	@Before
	public void setUp() throws IOException {
		this.tmpDir = createTmpDir();

		// Start a standalone local zookeeper server.
		this.port = PortAssignment.unique();
		logger.info("Starting ZooKeeper Standalone Server: " + this.port);
		this.zkServer = new StandaloneServer(this.port, tmpDir);
		this.zkServer.start();
		logger.info("ZooKeeper server started");
	}

	@After
	public void tearDown() {
		this.zkServer.shutdown();
		if (tmpDir != null) {
			recursiveDelete(tmpDir);
		}
	}

	/**
	 * This method stops a zookeeper server and
	 * starts a new one. This is to simulate in 
	 * tests a zookeeper server going on and off.
	 * 
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void restartServer() throws IOException, InterruptedException {
		this.zkServer.shutdown();
		File confFile = this.zkServer.confFile;
		//Thread.sleep( 1000 );
		this.zkServer = new StandaloneServer(confFile);
		this.zkServer.start();
	}

	/**
	 * Creates a temporary directory.
	 * 
	 * @return
	 * @throws IOException
	 */
	public static File createTmpDir() throws IOException {
		return createTmpDir(BASETEST);
	}

	/**
	 * Creates a temporary directory under a base directory.
	 * 
	 * @param parentDir
	 * @return
	 * @throws IOException
	 */
	static File createTmpDir(File parentDir) throws IOException {
		File tmpFile = File.createTempFile("test", ".junit", parentDir);
		// don't delete tmpFile - this ensures we don't attempt to create
		// a tmpDir with a duplicate name
		File tmpDir = new File(tmpFile + ".dir");
		Assert.assertFalse(tmpDir.exists()); // never true if tmpfile does it's job
		Assert.assertTrue(tmpDir.mkdirs());

		return tmpDir;
	}

	/**
	 * Deletes recursively.
	 * 
	 * @param d
	 * @return
	 */
	public static boolean recursiveDelete(File d) {
		if (d.isDirectory()) {
			File children[] = d.listFiles();
			for (File f : children) {
				Assert.assertTrue("delete " + f.toString(), recursiveDelete(f));
			}
		}
		return d.delete();
	}

}
