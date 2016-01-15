package mem.test.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class MemTest {
	private static final Log LOG = LogFactory.getLog(MemTest.class);
	private static final String AR_DEFAULT_SCHEMA_KEY = "ar";
	
	private static Map<String, DataSource> instanceMap = new HashMap<String, DataSource>();
	
	/**
	 * Don't let anyone instantiate me
	 */
	private MemTest() {}
	
	/**
	 * Delete the dart tables using the given datasource.
	 * @param ds the datasource to use
	 */
	public static void cleanDartTables(final DataSource ds) throws IOException {
		cleanARTables(new JdbcTemplate(ds));
	}
	
	/**
	 * Delete the dart tables using the given template.
	 * @param t the template to use
	 */
	public static void cleanARTables(final JdbcTemplate t) throws IOException {
		ExecuteSQL.executeSQLFile("deleteDartTables.sql", t);
	}
	
	/**
	 * Get the jdbctemplate with the default name.
	 * @return the jdbctemplate with the default name
	 */
	public static JdbcTemplate getARTemplate() throws IOException {
		return getARTemplate(AR_DEFAULT_SCHEMA_KEY);
	}
	
	/**
	 * Get the jdbctemplate with the given.
	 * @param name the name of the template to get
	 * @return the JDBC template with the given name
	 */
	public static JdbcTemplate getARTemplate(final String name) throws IOException {
		return new TestJdbcTemplateWrapper(new JdbcTemplate(getARDataSource(name)));
	}
	
	/**
	 * Get the datasource with the default name.
	 * @return the datasource with the default name
	 */
	public static DataSource getDartDataSource() throws IOException {
		return getARDataSource(AR_DEFAULT_SCHEMA_KEY);
	}
	
	/**
	 * Create a new datasource with the given name if one doesn't exist.
	 * @param name the name of the database to get
	 * @return the datasource with the given name
	 */
	public synchronized static DataSource getARDataSource(final String name) throws IOException {
		DataSource ds = instanceMap.get(name);
		
		// Dirty but just get out if we have one
		if (ds != null) {
			return ds;
		}
		
		ds = createDataSource(name);
		instanceMap.put(name, ds);
		
		return ds;
	}
	
	/**
	 * Create all of the tables needed for DART in memory.
	 * @param t the jdbctemplate to use
	 */
	public static void createARTables(final JdbcTemplate template) throws IOException {
    	ExecuteSQL.executeSQLFile("createDartTables.sql", template);
        ExecuteSQL.executeSQLFile("createDartFunctions.sql", template);
//        ExecuteSQL.executeSQLFile("dual.sql", template);
//		InitializeDataSource.createTables(template);
        InitializeDataSource.loadTables(template);
        
	}

	/**
	 * Create a new datasource for DART with the given name.
	 * @param name the name of the datasource to create
	 * @return the newly created datasource.
	 */
	private static DataSource createDataSource(final String name) throws IOException {
		LOG.info("creating dart datasource " + name);
		
		DataSource ds = createNewDatabase(name);
		JdbcTemplate t = new TestJdbcTemplateWrapper(new JdbcTemplate(ds));
		
		createARTables(t);
		
		return ds;
	}
	
	/**
	 * Create a new database in memory with the given schema name.
	 * @param schemaName the name of the schema to create
	 * @return the datasource for the newly created database
	 */
	private static DataSource createNewDatabase(final String schemaName) {
        String url = "jdbc:hsqldb:mem:%s;sql.syntax_ora=true";
        url = String.format(url, schemaName);

        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName("org.hsqldb.jdbcDriver");
        bds.setUrl(url);
        bds.setUsername("sa");
        bds.setPassword("");

        return bds;
    }
}
