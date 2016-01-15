package mem.test.db;

import java.io.File;
import java.io.IOException;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Class to initialize a datasource
 * 
 * @author Jim Kerwood
 * 
 */
public final class InitializeDataSource {

	private static final String DART_DATABASE_SCHEMA = "dart";

	private static final Object LOCK_OBJECT = new Object();

	// private static final Log LOG =
	// LogFactory.getLog(InitializeDataSource.class);

	/**
     *
     */
	private InitializeDataSource() {
	}

	/**
	 * @return an initialized datasource
	 * @throws Exception
	 *             if there was a problem loading the datasource
	 */
	public static JdbcTemplate getInitializedDartDataSource() {
		JdbcTemplate template = null;
		return initialize(template, false, DART_DATABASE_SCHEMA);
	}

	/**
	 * @param getMSA
	 *            if true gets the MSA data <br/>
	 *            NOTE: needs a HTTP connection to a intranet URL
	 * @return an initialized datasource
	 * @throws Exception
	 *             if there was a problem loading the datasource
	 */
	public static JdbcTemplate getInitializedDataSourceWithTestData() {
		try {
			JdbcTemplate template = InitializeDataSource.initializeWithTestData(null);

			return template;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Will create tables and setup type tables.
	 * 
	 * @param template
	 *            template used to initialize
	 * @throws Exception
	 *             if there was a problem with setup
	 */
	public static JdbcTemplate initialize(final JdbcTemplate template) {
		return initialize(template, false, DART_DATABASE_SCHEMA);
	}

	/**
	 * Will create tables, setup type tables and populate test data.
	 * 
	 * @param template
	 *            template used to initialize
	 * @throws Exception
	 *             if there was a problem with setup
	 */
	public static JdbcTemplate initializeWithTestData(final JdbcTemplate template) {
		return initialize(template, true, DART_DATABASE_SCHEMA);
	}

	/**
	 * Will create tables, setup type tables and populate test data.
	 * 
	 * @param template
	 *            template used to initialize
	 * @throws Exception
	 *             if there was a problem with setup
	 * @param getMSA
	 *            if true gets the MSA data <br/>
	 *            NOTE: needs a HTTP connection to a intranet URL
	 * @param withTestData
	 *            if true will setup test data
	 */
	private static JdbcTemplate initialize(final JdbcTemplate jdbcTemplate,
			final boolean withTestData,
			final String schema) {

		try {
			synchronized (LOCK_OBJECT) {
				JdbcTemplate template = jdbcTemplate;
				if (template == null) {
					template = createNewDatabase(schema);
				}
				else {
					template = new TestJdbcTemplateWrapper(template);
				}

				if (!isTablesCreated(template)) {
					createTables(template);
					loadTables(template);
				}

				if (withTestData && !isTestDataLoaded(template)) {
					// TODO: do we want default test data at some point?
					// setupTestData(template);
				}

				return template;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public static JdbcTemplate createNewDatabase(final String schemaName) {
		String url = "jdbc:hsqldb:mem:%s";
		url = String.format(url, schemaName);

		BasicDataSource bds = new BasicDataSource();
		bds.setDriverClassName("com.jpmc.cto.dart.test.db.HsqlSqlDriver");
		bds.setUrl(url);
		bds.setUsername("sa");
		bds.setPassword("");

		return new TestJdbcTemplateWrapper(new JdbcTemplate(bds));
	}

	/**
	 * @param template
	 *            template to check
	 * @return true if template has test data
	 */
	private static boolean isTestDataLoaded(JdbcTemplate template) {
		return template.queryForInt("select count(1) from vpc_sequence") > 0;
	}

	/**
	 * @param template
	 *            template to check
	 * @return true if template has test data
	 */
	private static boolean isDartTablesLoaded(JdbcTemplate template) {
		return template.queryForInt("select count(1) from REMEDIATION_TYPE") > 0;
	}

	/**
	 * @param template
	 *            template to check for table creation
	 * @return true if tables are there
	 */
	private static boolean isTablesCreated(JdbcTemplate template) {
		try {
			template.queryForInt("select 1 from dual");
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Will create tables.
	 * 
	 * @param template
	 *            template used to create tables
	 * @throws Exception
	 *             if there was a problem with setup
	 */
	public static void createTables(final JdbcTemplate template) throws Exception {
		ExecuteSQL.executeSQLFile("createDartTables.sql", template);
		ExecuteSQL.executeSQLFile("createDartFunctions.sql", template);
		ExecuteSQL.executeSQLFile("dual.sql", template);
	}
}