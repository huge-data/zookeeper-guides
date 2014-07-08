package zx.soft.zk.manager.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public enum CmdUtil {

	INSTANCE;
	//	private final static Logger logger = LoggerFactory.getLogger(CmdUtil.class);

	public String executeCmd(String cmd, String zkServer, String zkPort) throws IOException, InterruptedException {
		String[] cmdArr = { "/bin/sh", "-c", "echo " + cmd + " | nc -q5 " + zkServer + " " + zkPort };
		Process p = Runtime.getRuntime().exec(cmdArr);
		p.waitFor();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = reader.readLine();
		StringBuilder sb = new StringBuilder();
		while (line != null) {
			sb.append(line);
			sb.append("<br/>");
			line = reader.readLine();
		}
		return sb.toString();
	}

}
