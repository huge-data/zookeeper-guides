package zx.soft.zk.manager.listener;

import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebListener
public class SessionListener implements HttpSessionListener {

	private static final Logger logger = LoggerFactory.getLogger(SessionListener.class);

	@Override
	public void sessionCreated(HttpSessionEvent event) {
		logger.debug("Session created");
	}

	@Override
	public void sessionDestroyed(HttpSessionEvent event) {
		try {
			ZooKeeper zk = (ZooKeeper) event.getSession().getAttribute("zk");
			zk.close();
			logger.debug("Session destroyed");
		} catch (InterruptedException ex) {
			logger.error(ex.getMessage());
		}
	}

}
