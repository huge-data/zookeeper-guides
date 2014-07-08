package zx.soft.zk.manager.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.dao.Dao;
import zx.soft.zk.manager.domain.History;
import zx.soft.zk.manager.utils.ServletUtil;
import freemarker.template.TemplateException;

@WebServlet(urlPatterns = { "/history" })
public class ChangeLog extends HttpServlet {

	private static final long serialVersionUID = -946658849902937386L;

	private final static Logger logger = LoggerFactory.getLogger(ChangeLog.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.debug("History Get Action!");
		try {
			Properties globalProps = (Properties) this.getServletContext().getAttribute("globalProps");
			Dao dao = new Dao(globalProps);
			Map<String, Object> templateParam = new HashMap<>();
			List<History> historyLst = dao.fetchHistoryRecords();
			templateParam.put("historyLst", historyLst);
			templateParam.put("historyNode", "");
			ServletUtil.INSTANCE.renderHtml(request, response, templateParam, "history.ftl.html");
		} catch (TemplateException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
			IOException {
		logger.debug("History Post Action!");
		try {
			Properties globalProps = (Properties) this.getServletContext().getAttribute("globalProps");
			Dao dao = new Dao(globalProps);
			Map<String, Object> templateParam = new HashMap<>();
			String action = request.getParameter("action");
			List<History> historyLst;
			if (action.equals("showhistory")) {

				String historyNode = request.getParameter("historyNode");
				historyLst = dao.fetchHistoryRecordsByNode("%" + historyNode + "%");
				templateParam.put("historyLst", historyLst);
				templateParam.put("historyNode", historyNode);
				ServletUtil.INSTANCE.renderHtml(request, response, templateParam, "history.ftl.html");

			} else {
				response.sendRedirect("/history");
			}
		} catch (TemplateException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

}
