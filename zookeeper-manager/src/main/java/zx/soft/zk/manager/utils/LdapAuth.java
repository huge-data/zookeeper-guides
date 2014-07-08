package zx.soft.zk.manager.utils;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.slf4j.LoggerFactory;

public class LdapAuth {

	DirContext ctx = null;
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(LdapAuth.class);

	public boolean authenticateUser(String ldapUrl, String username, String password, String domains) {

		String[] domainArr = domains.split(",");
		for (String domain : domainArr) {
			Hashtable<String, String> env = new Hashtable<>();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.PROVIDER_URL, ldapUrl);
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			env.put(Context.SECURITY_PRINCIPAL, domain + "\\" + username);
			env.put(Context.SECURITY_CREDENTIALS, password);
			try {
				ctx = new InitialDirContext(env);
				return true;
			} catch (NamingException e) {

			} finally {
				if (ctx != null) {
					try {
						ctx.close();
					} catch (NamingException ex) {
						logger.warn(ex.getMessage());
					}
				}
			}
		}
		return false;
	}

}
