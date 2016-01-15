package mem.test.db;

import java.io.IOException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Server;
import org.springframework.jdbc.core.JdbcTemplate;

public class HsqlDbStart {
	private static final Log LOG = LogFactory.getLog(HsqlDbStart.class);
	private static final String HSQL_THREAD_NAME = "HSQL_DB_THREAD";

	private static Thread HSQL_THREAD;
	private static JdbcTemplate HSQL_TEMPLATE;
	private static DataSource HSQL_DATASOURCE;

	public static void main(String... args) throws InterruptedException, IOException {
		startHSQLDB();
	}

	public static void startHSQLDB() throws InterruptedException, IOException {
		if (!isHSQLDBRunning()) {
			HSQL_THREAD = new Thread(HSQL_THREAD_NAME) {
				public void run() {
					Server.main(new String[] { "-database.0", "mem:ar;sql.syntax_ora=true",
							"-dbname.0", "ar" });
				}
			};
			HSQL_THREAD.start();

			LOG.info("Initing HSQL DB");
			HSQL_DATASOURCE = MemTest.getARDataSource("ar");
			HSQL_TEMPLATE = MemTest.getARTemplate("ar");
			Thread.sleep(3000); // Give the DB time to startup
			LOG.info("Ready to start app");
			
			// need sql file here
			loadHSQL("");
			HSQL_THREAD.join();
		}
	}

	public static boolean isHSQLDBRunning() {
		return (HSQL_THREAD != null && HSQL_THREAD.isAlive());
	}

	public static Thread getHSQLThread() {
		return HSQL_THREAD;
	}

	public static JdbcTemplate getHSQLTemplate() {
		return HSQL_TEMPLATE;
	}

	public static DataSource getHSQLDataSource() {
		return HSQL_DATASOURCE;
	}

	public static void loadHSQL(String sqlFile) throws IOException {
		ExecuteSQL.executeSQLFile(sqlFile, HSQL_TEMPLATE);
	}
}
