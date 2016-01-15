package mem.test.db;

import java.io.IOException;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Class to initialize a datasource
 */

public final class InitializeDataSource {
	private static final String DART_DATABASE_SCHEMA = "mem";

	private static final Object LOCK_OBJECT = new Object();

	private InitializeDataSource() {
	}

	// return an initialized datasource
	public static JdbcTemplate getInitializedDartDataSource() {
		JdbcTemplate template = null;
		return initialize(template, false, DART_DATABASE_SCHEMA);
	}

	/**
	 * @param getMSA
	 *            if true gets the MSA data Note: needs a HTTP connection to a
	 *            intranet URL
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
	 * will create tables and setup type tables.
	 * 
	 * @param template
	 *            template used to initialized
	 * @throws Exception
	 *             if there was a problem with setup
	 */
	public static JdbcTemplate initialize(final JdbcTemplate template) {
		return initialize(template, false, DART_DATABASE_SCHEMA);
	}

	/**
	 * will create tables, and setup type tables and populate test data.
	 * 
	 * @param template
	 *            template used to initialized
	 * @throws Exception
	 *             if there was a problem with setup
	 */
	public static JdbcTemplate initializeWithTestData(final JdbcTemplate template) {
		return initialize(template, true, DART_DATABASE_SCHEMA);
	}

	/**
	 * will create tables, and setup type tables and populate test data.
	 * 
	 * @param template
	 *            template used to initialized
	 * @throws Exception
	 *             if there was a problem with setup
	 * @param getMSA
	 *            if true gets the MSA data Note: needs a HTTP connection to a
	 *            intranet URL
	 * @param withTestData
	 *            if true will setup test data
	 */

	private static JdbcTemplate initialize(final JdbcTemplate jdbcTemplate,
			final boolean withTestData, final String schema) {

		try {
			synchronized (LOCK_OBJECT) {
				JdbcTemplate template = jdbcTemplate;
				if (template == null) {
					template = createNewDatabase(schema);
				} else {
					template = new TestJdbcTemplateWrapper(template);
				}

				if (!isTablesCreated(template)) {
					createTables(template);
					loadTables(template);
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
		bds.setDriverClassName("mem.test.db.HsqlSqlDriver");
		bds.setUrl(url);
		bds.setUsername("sa");
		bds.setPassword("");

		return new TestJdbcTemplateWrapper(new JdbcTemplate(bds));
	}

	private static boolean isTestDataLoaded(JdbcTemplate template) {
		return template.queryForObject("select count(1) from REMEDIATION TYPE", Integer.class) > 0;
	}

	private static boolean isDartTablesLoaded(JdbcTemplate template) {
		return template.queryForObject("select count(1) from REMEDIATION TYPE", Integer.class) > 0;
	}

	private static boolean isTablesCreated(JdbcTemplate template) {
		try {
			template.queryForObject("select count(1) from REMEDIATION TYPE", Integer.class);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static void createTables(final JdbcTemplate template) throws Exception {
		ExecuteSQL.executeSQLFile("createDartTables.sql", template);
		ExecuteSQL.executeSQLFile("createDartFunctions.sql", template);
		ExecuteSQL.executeSQLFile("dual.sql", template);
	}

	public static void loadTables(final JdbcTemplate template) throws IOException {
		if (!isDartTablesLoaded(template)) {
			ExecuteSQL.executeSQLFile("loadDartTables.sql", template);

			// configuration table needs to be populated with the following 3
			// image configs

			// image data
			template.update("INSERT INTO CONFIGURATION VALUES(6, 'IMAGE_DATA')");
			// grey image data
			template.update("INSERT INTO CONFIGURATION VALUES(14, 'GREY_IMAGE_DATA')");
			// thumbnail image data
			template.update("INSERT INTO CONFIGURATION VALUES(16, 'THUMBNAIL_IMAGE_DATA')");
		}
	}

}