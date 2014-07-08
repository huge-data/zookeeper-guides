package zx.soft.zk.manager.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.utils.CmdUtil;
import zx.soft.zk.manager.utils.ServletUtil;
import freemarker.template.TemplateException;

@WebServlet(urlPatterns = { "/monitor" })
public class Monitor extends HttpServlet {

	private static final long serialVersionUID = -992002919738191063L;

	private final static Logger logger = LoggerFactory.getLogger(Monitor.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.debug("Monitor Action!");
		try {
			Properties globalProps = (Properties) this.getServletContext().getAttribute("globalProps");
			String zkServer = globalProps.getProperty("zkServer");
			String[] zkServerLst = zkServer.split(",");

			Map<String, Object> templateParam = new HashMap<>();
			StringBuffer stats = new StringBuffer();
			for (String zkObj : zkServerLst) {
				stats.append("<br/><hr/><br/>").append("Server: ").append(zkObj).append("<br/><hr/><br/>");
				String[] monitorZKServer = zkObj.split(":");
				stats.append(CmdUtil.INSTANCE.executeCmd("stat", monitorZKServer[0], monitorZKServer[1]));
				stats.append(CmdUtil.INSTANCE.executeCmd("envi", monitorZKServer[0], monitorZKServer[1]));
			}
			templateParam.put("stats", stats);
			ServletUtil.INSTANCE.renderHtml(request, response, templateParam, "monitor.ftl.html");

		} catch (IOException | InterruptedException | TemplateException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

}
