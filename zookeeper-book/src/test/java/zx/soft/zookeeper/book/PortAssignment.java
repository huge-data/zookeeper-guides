package zx.soft.zookeeper.book;

/** Assign ports to tests */
public class PortAssignment {

	private static int nextPort = 11221;

	/** Assign a new, unique port to the test */
	public synchronized static int unique() {
		return nextPort++;
	}

}