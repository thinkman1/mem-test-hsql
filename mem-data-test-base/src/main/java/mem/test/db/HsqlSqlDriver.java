/**
 * 
 */
package mem.test.db;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@SuppressWarnings("unchecked")
public class HsqlSqlDriver extends org.hsqldb.jdbcDriver implements Driver {

	private static final Map<String, String> oldToNewMap = new HashMap<String, String>();
	private static final Log log = LogFactory.getLog(HsqlSqlDriver.class);

	static {
		synchronized (oldToNewMap) {
			try {
				DriverManager.registerDriver(new HsqlSqlDriver());
				Driver d = DriverManager.getDriver("jdbc:hsqldb:nomatter");
				DriverManager.deregisterDriver(d);
			} catch (Exception e) {
			}

			try {
				ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
						"hsqlTransform.xml");
				Map<String, String> ctxMap = (Map<String, String>) ctx.getBean("oldToNewMap");

				Set<String> keySet = ctxMap.keySet();
				for (String key : keySet) {
					String sql = ctxMap.get(key);
					String normalizeSql = normalizeSql(key);
					oldToNewMap.put(normalizeSql, sql);
					log.debug("Ready to proxy " + normalizeSql + " sql to " + sql);
				}
				log.info("Ready to proxy " + keySet.size() + " sqls");
			} catch (Exception e) {
				log.trace("Could not proxy sqls " + e.getMessage());
			}
		}
	}

	@Override
	public Connection connect(String arg0, Properties arg1) throws SQLException {

		final Connection connect = super.connect(arg0, arg1);
		Connection c = (Connection) Proxy.newProxyInstance(HsqlSqlDriver.class.getClassLoader(),
				new Class[] { Connection.class },
				new InvocationHandler() {

					@Override
					public Object invoke(Object proxy, Method method, Object[] args)
							throws Throwable {
						Object invoke = method.invoke(connect, fixArgs(args));
						if (method.getName().equals("createStatement")) {
							Statement s = (Statement) Proxy.newProxyInstance(
									HsqlSqlDriver.class.getClassLoader(),
									new Class[] { Statement.class }, new ProxySql(invoke));
							return s;
						}
						return invoke;
					}
				});

		return c;
	}

	private static String normalizeSql(String key) {
		String deleteWhitespace = StringUtils.deleteWhitespace(key);
		return deleteWhitespace.toLowerCase();
	}

	private static final class ProxySql implements InvocationHandler {
		private Object object;

		public ProxySql(Object invoke) {
			this.object = invoke;
		}

		@Override
		public Object invoke(Object proxy, Method method,
				Object[] args) throws Throwable {
			return method.invoke(object, fixArgs(args));
		}

	}

	private static Object[] fixArgs(Object[] args) {
		boolean changed = false;
		if (args != null && args.length == 1 && args[0].getClass() == String.class) {
			String origSql = normalizeSql(args[0].toString());
			if (oldToNewMap.containsKey(origSql)) {
				args[0] = oldToNewMap.get(origSql);
				changed = true;
			} else if (log.isDebugEnabled()) {
				log.debug("Didn't find a match for " + origSql);
			}
		}

		if (changed && log.isInfoEnabled()) {
			log.info("Changed sql to: " + args[0]);
		}

		return args;
	}

	public Map<String, String> getOldToNewMap() {
		return oldToNewMap;
	}

}
