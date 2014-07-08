package zx.soft.zk.manager.controller;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.utils.ServletUtil;

@WebServlet(urlPatterns = { "/logout" })
public class Logout extends HttpServlet {

	private static final long serialVersionUID = -1748766483057385417L;

	private final static Logger logger = LoggerFactory.getLogger(Logout.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			logger.debug("Logout Action!");
			Properties globalProps = (Properties) getServletContext().getAttribute("globalProps");
			String zkServer = globalProps.getProperty("zkServer");
			String[] zkServerLst = zkServer.split(",");
			ZooKeeper zk = ServletUtil.INSTANCE.getZookeeper(request, response, zkServerLst[0]);
			request.getSession().invalidate();
			zk.close();
			response.sendRedirect("/login");
		} catch (InterruptedException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}

	}
}
