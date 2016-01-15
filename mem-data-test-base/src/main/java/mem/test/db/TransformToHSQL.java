package mem.test.db;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms oracle like sql to hsql
 * 
 */
public final class TransformToHSQL {

	private static final String ADD = "add";

	private static final Pattern createSequencePattern = Pattern.compile(
			"(?i)^(create\\s+sequence\\s+\\w+\\s).*", Pattern.DOTALL);

	private static final Pattern nextSequencePattern = Pattern.compile(
			"(?i).*?\\W+((\\w+)(\\.nextval))\\W+.*?"
			, Pattern.DOTALL
			);

	private static final String NEW_SEQUENCE_PATTERN = "NEXT VALUE FOR %s";

	/**
	 * Yikes! The 'count(*) over()' is really useful but HSQL doesn't support
	 * it. Just going to take it out for now.
	 */
	private static final Pattern countOverPattern = Pattern.compile(
			"(?i).*?(count\\(.*?\\) over\\(.*?\\)).*?"
			, Pattern.DOTALL
			);

	private static final String COUNT_OVER_HARDCODE = "5";

	/**
	 * HSQL wants 'ROWNUM()' instead of 'rownum'. Make sure there is whitespace,
	 * beginning of line or end of line
	 */
	private static final Pattern rownumPattern = Pattern.compile(
			"(?i).*?((\\s+?)(rownum)(\\s+?)).*?"
			, Pattern.DOTALL
			);

	private static final String ROWNUM_COMMAND = "ROWNUM()";

	private static final String SEMI = ";";

	/**
	 * Private constructor
	 */
	private TransformToHSQL() {
	}

	/**
	 * @param oracleSql
	 *            oracle syntax to change
	 * @return sql string of HSQL sql
	 * @throws IOException
	 *             if there was a problem
	 */
	public static String transformSQL(final String oracleSql) {

		String updatedSQL = handleSQLLevelIssues(oracleSql);
		updatedSQL = handleTypes(updatedSQL);

		return updatedSQL;
	}

	/**
	 * Works on single sqls by splitting on ;
	 * 
	 * @param sql
	 *            full sql
	 * @return full sql fixed
	 */
	private static String handleSQLLevelIssues(final String sql) {
		String[] sqls = sql.split(SEMI);
		StringBuilder builder = new StringBuilder();
		for (int sqlIndex = 0; sqlIndex < sqls.length; sqlIndex++) {
			String singleStatement = sqls[sqlIndex].trim();

			singleStatement = handleCreateSequence(singleStatement);
			singleStatement = handleGrants(singleStatement);
			singleStatement = handleAlterTableAdd(singleStatement);
			singleStatement = handleDateForDML(singleStatement);
			singleStatement = handleSysdate(singleStatement);
			singleStatement = handleCreateDateTypes(singleStatement);
			singleStatement = handleCountOver(singleStatement);
			singleStatement = handleRownum(singleStatement);

			builder.append(singleStatement);
			builder.append(SEMI);
		}
		return builder.toString();
	}

	/**
	 * @param singleStatement
	 *            statement to check/ fix
	 * @return a statement with a timestamp replacement of dates in case of
	 *         create table otherwise same as input
	 */
	private static String handleCreateDateTypes(final String singleStatement) {
		String returnMe = singleStatement;
		if (singleStatement.toLowerCase().contains("create table")) {
			returnMe = singleStatement.replaceAll("(?i)\\s+date\\s+",
					" timestamp ");
		}
		return returnMe;
	}

	/**
	 * Fixes sysdate
	 * 
	 * @param singleStatement
	 *            single statement to fix
	 * @return fixed statement
	 */
	private static String handleSysdate(final String singleStatement) {
		String returnString = singleStatement;
		String lowerCase = singleStatement.toLowerCase();
		if (lowerCase.contains("sysdate")) {
			returnString = singleStatement.replaceAll("(?i)\\s+sysdate\\s+",
					" now ");
		}
		return returnString;
	}

	/**
	 * handles dates for ins upd del
	 * 
	 * @param singleStatement
	 *            statement to fix / check
	 * @return fixed sql or same sql
	 */
	private static String handleDateForDML(final String singleStatement) {
		String returnString = singleStatement;
		String lowerCase = singleStatement.toLowerCase();
		if (lowerCase.startsWith("insert") || lowerCase.startsWith("update")
				|| lowerCase.startsWith("delete")) {
			returnString = singleStatement.replaceAll("(?i)\\s+date\\s+'", "'");
		}
		return returnString;
	}

	/**
	 * Fixes alter table statements
	 * 
	 * @param singleStatement
	 *            statement to check / fix
	 * @return fixed statement or same statement
	 */
	private static String handleAlterTableAdd(final String singleStatement) {
		String returnStatement = singleStatement;
		String lowerCase = singleStatement.toLowerCase();
		if (lowerCase.startsWith("alter table") && lowerCase.contains(ADD)) {
			int indexAfterAdd = lowerCase.indexOf(ADD) + ADD.length();
			String preAdd = returnStatement.substring(0, indexAfterAdd);
			String postAdd = returnStatement.substring(indexAfterAdd,
					returnStatement.length());
			returnStatement = preAdd;
			postAdd = postAdd.replaceFirst("\\(", "");
			postAdd = postAdd.replaceFirst("\\)\\s*$", "");
			returnStatement += postAdd;
		}
		return returnStatement;
	}

	/**
	 * Removes grants
	 * 
	 * @param singleStatement
	 *            statement to check for grants
	 * @return empty string if a grant same statement otherwise
	 */
	private static String handleGrants(final String singleStatement) {
		String returnStatement = singleStatement;
		if (singleStatement.toLowerCase().startsWith("grant")) {
			returnStatement = "";
		}
		return returnStatement;
	}

	/**
	 * @param singleStatement
	 *            statement to check
	 * @return fixes the sequence
	 */
	private static String handleCreateSequence(final String singleStatement) {
		String returnStatement = singleStatement;
		Matcher m = createSequencePattern.matcher(singleStatement);
		if (m.matches()) {
			returnStatement = m.group(1) + "START WITH 85";
		}
		return returnStatement;
	}

	/**
	 * Find instances of '[sequence name].nextval' and replace it with HSql's
	 * (and ANSI SQL's) syntax of 'NEXT VALUE OF [sequence name]'. <br />
	 * <br />
	 * 
	 * Note: extra variables were put in there on purpose for debugging. It
	 * should not hurt performance.
	 * 
	 * @param query
	 *            Query that may or may not contain an Oracle sequence.
	 * 
	 * @return patched-up query
	 */
	public static String handleReplaceSequence(String query) {
		Matcher m = nextSequencePattern.matcher(query);
		if (m.matches()) {
			// Group 1 should be the sequence name with '.nextval'
			// Group 2 should be just the sequence name
			// If this is wrong we need someone to look at it so just let the
			// exception bubble up.
			String sequenceWithNextVal = m.group(1);
			String sequenceName = m.group(2);
			String hsqlSequence = String.format(NEW_SEQUENCE_PATTERN, sequenceName);
			String newString = query.replaceAll(sequenceWithNextVal, hsqlSequence);
			return handleReplaceSequence(newString);
		}
		return query;
	}

	/**
	 * Find instances of 'count(*) over ()' and replace it with just a number. <br />
	 * <br />
	 * 
	 * Note: extra variables were put in there on purpose for debugging. It
	 * should not hurt performance.
	 * 
	 * @param query
	 *            Query that may or may not contain an Oracle's count over
	 * 
	 * @return patched-up query
	 */
	public static String handleCountOver(String query) {
		Matcher m = countOverPattern.matcher(query);
		if (m.matches()) {
			// Group 1 is the 'count over'
			// If this is wrong we need someone to look at it so just let the
			// exception bubble up.
			String countOver = m.group(1);
			String newString = query.replace(countOver, COUNT_OVER_HARDCODE);
			return handleCountOver(newString);
		}
		return query;
	}

	/**
	 * Find instances of 'rownum' and replace it with 'ROWNUM()'. <br />
	 * <br />
	 * 
	 * Note: extra variables were put in there on purpose for debugging. It
	 * should not hurt performance.
	 * 
	 * @param query
	 *            Query that may or may not contain an Oracle's 'rownum'
	 * 
	 * @return patched-up query
	 */
	public static String handleRownum(String query) {
		Matcher m = rownumPattern.matcher(query);
		if (m.matches()) {
			// Group 1 is the white space before/after and the 'rownum'. We're
			// going to replace it all
			// Group 2 is the white space before the 'rownum'
			// Group 3 is the actual 'rownum'
			// Group 4 is the white space after the 'rownum'
			// If this is wrong we need someone to look at it so just let the
			// exception bubble up.
			String entirematch = m.group(1);
			String beforeRownum = m.group(2);
			// String rownumOnly = m.group(3);
			String afterRownum = m.group(4);
			String newString = query.replace(entirematch, beforeRownum + ROWNUM_COMMAND
					+ afterRownum);
			return handleRownum(newString);
		}
		return query;
	}

	/**
	 * @param sql
	 *            sql to change
	 * 
	 * @return full sql fixed of such things as varchar2, timestamps and number
	 */
	private static String handleTypes(final String sql) {
		String returnString = sql.replaceAll("(?i)\\svarchar2", " varchar");
		returnString = returnString.replaceAll("(?i)\\stimestamp\\(\\d\\)",
				" timestamp");
		returnString = returnString.replaceAll("(?i)\\snumber\\(\\d,\\d\\)", " float");
		returnString = returnString.replaceAll("(?i)\\snumber", " numeric");
		returnString = returnString.replaceAll("(?i)\\sinteger", " bigint");
		returnString = returnString.replaceAll("(?i)\\sclob\\s", " LONGVARCHAR ");

		return returnString;
	}
}
