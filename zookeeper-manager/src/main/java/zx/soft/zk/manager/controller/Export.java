package zx.soft.zk.manager.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.domain.LeafBean;
import zx.soft.zk.manager.utils.ServletUtil;
import zx.soft.zk.manager.utils.ZooKeeperUtil;

@WebServlet(urlPatterns = { "/export" })
public class Export extends HttpServlet {

	private static final long serialVersionUID = 8776755363631646813L;

	private final static Logger logger = LoggerFactory.getLogger(Export.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.debug("Export Get Action!");
		try {
			Properties globalProps = (Properties) this.getServletContext().getAttribute("globalProps");
			String zkServer = globalProps.getProperty("zkServer");
			String[] zkServerLst = zkServer.split(",");

			String authRole = (String) request.getSession().getAttribute("authRole");
			if (authRole == null) {
				authRole = ZooKeeperUtil.ROLE_USER;
			}
			String zkPath = request.getParameter("zkPath");
			StringBuilder output = new StringBuilder();
			output.append("#App Config Dashboard (ACD) dump created on :").append(new Date()).append("\n");
			Set<LeafBean> leaves = ZooKeeperUtil.INSTANCE.exportTree(zkPath,
					ServletUtil.INSTANCE.getZookeeper(request, response, zkServerLst[0]), authRole);
			for (LeafBean leaf : leaves) {
				output.append(leaf.getPath()).append('=').append(leaf.getName()).append('=')
						.append(ServletUtil.INSTANCE.externalizeNodeValue(new String(leaf.getValue()))).append('\n');
			}// for all leaves
			response.setContentType("text/plain");
			try (PrintWriter out = response.getWriter()) {
				out.write(output.toString());
			}
		} catch (InterruptedException | KeeperException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

}
