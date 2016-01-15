package mem.test.db;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

/**
 * wraper class to handle converting Oracle specific commands to HSQL (ANSI)
 * commands where possible.
 * Note that this test wrapper is not all encompassing. it's done just enough to help us get past our testing.
 * @author thinkman
 *
 */
public class TestJdbcTemplateWrapper extends JdbcTemplate{
	private JdbcTemplate wrappedTemplate;
	
	/*default - just call super
	 * 
	 */
	public TestJdbcTemplateWrapper(JdbcTemplate wrappedTemplate){
		this.wrappedTemplate = wrappedTemplate;
	}
	
	@Override
	public int[] batchUpdate(String sql, BatchPreparedStatementSetter pss) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.batchUpdate(newQuery, pss);
	}
	
	@Override
	public int[] batchUpdate(String[] sql) throws DataAccessException{
		String[] newQueries = (sql == null ? null : new String[sql.length]);
		
		if(newQueries != null){
			for(int i=0; i<sql.length; i++){
				newQueries[i] = this.convertSql(sql[i]);
			}
		}
				
		return this.wrappedTemplate.batchUpdate(newQueries);
	}
	
	@Override
	public Object execute(String callString, CallableStatementCallback action) throws DataAccessException{
		String newQuery = convertSql(callString);
		return this.wrappedTemplate.execute(newQuery, action);
	}
	
	@Override
	public Object execute(String sql, PreparedStatementCallback action) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.execute(newQuery, action);
	}
	
	@Override
	public void execute(String sql) throws DataAccessException{
		String newQuery = convertSql(sql);
		this.wrappedTemplate.execute(newQuery);
	}
	
	@Override
	public Object query(String sql, Object[] args, int[] argTypes, ResultSetExtractor rse) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, args, argTypes, rse);
	}
	
	@Override
	public void query(String sql, Object[] args, int[] argTypes, RowCallbackHandler rch) throws DataAccessException{
		String newQuery = convertSql(sql);
		this.wrappedTemplate.query(newQuery, args, argTypes, rch);
	}
	
	@Override
	public List<?> query(String sql, Object[] args, int[] argTypes, RowMapper rowMapper) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, args, argTypes, rowMapper);
	}
	
	@Override
	public Object query(String sql, Object[] args, ResultSetExtractor rse) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, args,  rse);
	}
	
	@Override
	public void query(String sql, Object[] args,  RowCallbackHandler rch) throws DataAccessException{
		String newQuery = convertSql(sql);
		this.wrappedTemplate.query(newQuery, args,  rch);
	}
	
	@Override
	public List<?> query(String sql, Object[] args,  RowMapper rowMapper) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, args,  rowMapper);
	}
	
	@Override
	public Object query(String sql, PreparedStatementSetter pss, ResultSetExtractor rse) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, pss,  rse);
	}
	
	@Override
	public void query(String sql, PreparedStatementSetter pss,  RowCallbackHandler rch) throws DataAccessException{
		String newQuery = convertSql(sql);
		this.wrappedTemplate.query(newQuery, pss,  rch);
	}
	
	@Override
	public List<?> query(String sql, PreparedStatementSetter pss,  RowMapper rowMapper) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, pss,  rowMapper);
	}
	
	
	@Override
	public Object query(String sql, ResultSetExtractor rse) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, rse);
	}
	
	@Override
	public void query(String sql, RowCallbackHandler rch) throws DataAccessException{
		String newQuery = convertSql(sql);
		this.wrappedTemplate.query(newQuery,   rch);
	}
	
	@Override
	public List<?> query(String sql,  RowMapper rowMapper) throws DataAccessException{
		String newQuery = convertSql(sql);
		return this.wrappedTemplate.query(newQuery, rowMapper);
	}
	
	
	/**
	 * Common place to convert the query from what we have to what HSQL likes
	 * @param callString
	 * @return
	 */
	protected String convertSql(String callString){
		String newQuery = TransformToHSQL.handleReplaceSequence(callString);
		newQuery = TransformToHSQL.handleCountOver(newQuery);
		newQuery = TransformToHSQL.handleRownum(newQuery);
		return newQuery;
	}
	
}
