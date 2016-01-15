/**
 * 
 */
package mem.test.db;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

/**
 * @author thinkman
 * 
 */
public class ExecuteSQL {

	private static final Log LOG = LogFactory.getLog(ExecuteSQL.class);

	private ExecuteSQL() {
	};
	
	public static void executeSQLFile(final String fileName, final JdbcTemplate template) throws IOException {
		executeSQLFile(fileName, template, true);
	}

	/**
	 * 
	 * @param fileName
	 *            The name of the sql script file to run
	 * @param template
	 *            A JdbcTemplate to use
	 * @param removeDrops
	 *            will remove drop statements if true
	 * @throws IOException
	 */
	public static void executeSQLFile(final String fileName, final JdbcTemplate template,
			final boolean removeDrops) throws IOException {

		URL file = ExecuteSQL.class.getClassLoader().getResource(fileName);
		if (file == null) {
			throw new IOException("Could not find file with name " + fileName);
		}

		LOG.debug("File name = " + file.getFile());

		String sql = IOUtils.toString(ExecuteSQL.class.getClassLoader().getResourceAsStream(
				fileName));
		Connection c = null;

		try {
			c = template.getDataSource().getConnection();
			DatabaseMetaData meta = c.getMetaData();
			if (meta.getDatabaseProductName().startsWith("HSQL")) {
				sql = TransformToHSQL.transformSQL(sql);
			} else {
				throw new IOException("I dont think this should run if we are not hsql");
			}
		} catch (SQLException e) {
			LOG.warn("Had a problem determinig database type. Continuing");
		} finally {
			DataSourceUtils.releaseConnection(c, template.getDataSource());
		}

		String[] sqls = sql.split(";");
		int count = 0;
		for (int i = 0; i < sqls.length; i++) {
			if (sqls[i].trim().length() > 0) {
				String statement = sqls[i].trim();
				int spaceIndex = statement.indexOf(' ');
				if (spaceIndex < 0) {
					spaceIndex = statement.length();
				}

				String statementType = statement.substring(0, spaceIndex).toLowerCase();
				if (!(statementType.equals("drop") && removeDrops)) {
					try {
						count = template.update(sqls[i]);
					} catch (Exception e) {

					}

					if (statement.trim().toLowerCase().startsWith("insert")
							|| statement.trim().toLowerCase().startsWith("update")) {
						if (count < 1) {
							throw new IllegalStateException(
									"Should have been at least 1 row changed.  Instead update "
											+ count);
						}
					}
				}
			}
		}

	}
}
