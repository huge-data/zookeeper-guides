package zx.soft.zk.manager.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.zk.manager.utils.LdapAuth;
import zx.soft.zk.manager.utils.ServletUtil;
import zx.soft.zk.manager.utils.ZooKeeperUtil;
import freemarker.template.TemplateException;

@WebServlet(urlPatterns = { "/login" })
public class Login extends HttpServlet {

	private static final long serialVersionUID = -234472680942963300L;

	private final static Logger logger = LoggerFactory.getLogger(Login.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		logger.debug("Login Action!");
		try {
			Properties globalProps = (Properties) getServletContext().getAttribute("globalProps");
			Map<String, Object> templateParam = new HashMap<>();
			templateParam.put("uptime", globalProps.getProperty("uptime"));
			templateParam.put("loginMessage", globalProps.getProperty("loginMessage"));
			ServletUtil.INSTANCE.renderHtml(request, response, templateParam, "login.ftl.html");
		} catch (TemplateException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
			IOException {
		logger.debug("Login Post Action!");
		try {
			Properties globalProps = (Properties) getServletContext().getAttribute("globalProps");
			Map<String, Object> templateParam = new HashMap<>();
			HttpSession session = request.getSession(true);
			session.setMaxInactiveInterval(Integer.valueOf(globalProps.getProperty("sessionTimeout")));
			//TODO: Implement custom authentication logic if required.
			String username = request.getParameter("username");
			String password = request.getParameter("password");
			String role = null;
			Boolean authenticated = false;
			//if ldap is provided then it overrides roleset.
			if (globalProps.getProperty("ldapAuth").equals("true")) {
				authenticated = new LdapAuth().authenticateUser(globalProps.getProperty("ldapUrl"), username, password,
						globalProps.getProperty("ldapDomain"));
				if (authenticated) {
					JSONArray jsonRoleSet = (JSONArray) ((JSONObject) new JSONParser().parse(globalProps
							.getProperty("ldapRoleSet"))).get("users");
					for (Iterator<?> it = jsonRoleSet.iterator(); it.hasNext();) {
						JSONObject jsonUser = (JSONObject) it.next();
						if (jsonUser.get("username") != null && jsonUser.get("username").equals("*")) {
							role = (String) jsonUser.get("role");
						}
						if (jsonUser.get("username") != null && jsonUser.get("username").equals(username)) {
							role = (String) jsonUser.get("role");
						}
					}
					if (role == null) {
						role = ZooKeeperUtil.ROLE_USER;
					}

				}
			} else {
				JSONArray jsonRoleSet = (JSONArray) ((JSONObject) new JSONParser().parse(globalProps
						.getProperty("userSet"))).get("users");
				for (Iterator<?> it = jsonRoleSet.iterator(); it.hasNext();) {
					JSONObject jsonUser = (JSONObject) it.next();
					if (jsonUser.get("username").equals(username) && jsonUser.get("password").equals(password)) {
						authenticated = true;
						role = (String) jsonUser.get("role");
					}
				}
			}
			if (authenticated) {
				logger.info("Login successfull: " + username);
				session.setAttribute("authName", username);
				session.setAttribute("authRole", role);
				response.sendRedirect("/home");
			} else {
				session.setAttribute("flashMsg", "Invalid Login");
				ServletUtil.INSTANCE.renderHtml(request, response, templateParam, "login.ftl.html");
			}

		} catch (ParseException | TemplateException ex) {
			ServletUtil.INSTANCE.renderError(request, response, ex.getMessage());
		}
	}

}
