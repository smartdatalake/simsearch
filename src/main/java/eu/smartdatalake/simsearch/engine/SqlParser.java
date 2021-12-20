package eu.smartdatalake.simsearch.engine;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.request.SearchOutput;
import eu.smartdatalake.simsearch.request.SearchRequest;
import eu.smartdatalake.simsearch.request.SearchSpecs;

/**
 * A simplified parser for SQL-like SELECT statements against the local SimSearch instance.
 */
public class SqlParser {

	/** 
	 * Constructor
	 */
	public SqlParser() {
		
	}
	
	
	/**
	 * Parses a simple SQL-like SELECT statement.
	 * @param selectSQL  The SQL SELECT query to parse.
	 * @return  A JSON object corresponding to a valid search request for SimSearh.
	 */
	public SearchRequest parseSelect(String selectSQL) {

		// First rename custom SimSearch SQL-like clauses to standard ones amenable to SQL parsing.
		while (selectSQL.contains("~="))
			selectSQL = rename(selectSQL, "~=", " like ");
		selectSQL = rename(selectSQL, " weights ", " group by ");
		selectSQL = rename(selectSQL, " algorithm ", " having ");
		
		// Parse the SELECT statement and construct a search request for submission to the running SimSearch instance
		SearchRequest req = new SearchRequest();
		try {
            Statement select = (Statement) CCJSqlParserUtil.parse(selectSQL);
            // Attributes in SELECT may include extra attributes in existing data sources apart from those involved in similarity conditions 
            List<SelectItem> selectCols = ((PlainSelect) ((Select) select).getSelectBody()).getSelectItems();
            List<String> extraColumns = new ArrayList<String>();
            for (SelectItem selectItem : selectCols) {
                selectItem.accept(new SelectItemVisitorAdapter() {
                    @Override
                    public void visit(SelectExpressionItem item) {
                    	extraColumns.add(item.getExpression().toString().replaceAll("\'",""));  // Replace single quotes
                    }
                });
            }
          
            req.output = new SearchOutput();
            req.output.format = "console";  // Default output for SQL queries
            if (selectCols != null)
            	req.output.extra_columns = extraColumns.toArray(new String[0]);
            
            // Identify if weights have been assigned to each attribute involved in the query
            List<Expression> weightClause = ((PlainSelect) ((Select) select).getSelectBody()).getGroupByColumnReferences();
            
            // Specify the top-k value
            if (((PlainSelect) ((Select) select).getSelectBody()).getLimit() != null) 
            	req.k = Integer.parseInt(((PlainSelect) ((Select) select).getSelectBody()).getLimit().getRowCount().toString());
            else
            	req.k = Constants.K_MAX;

            // Detect if a ranking method has been specified; otherwise, use the default (threshold)
            Expression algorithmClause = ((PlainSelect) ((Select) select).getSelectBody()).getHaving(); 
            if (algorithmClause != null) {
	            if (Constants.RANKING_METHODS.contains(algorithmClause.toString().toLowerCase()))
	            	req.algorithm = algorithmClause.toString().toLowerCase();
            }
            
            // Handle attributes, their query values, and their weights
            Expression whereClause = ((PlainSelect) ((Select) select).getSelectBody()).getWhere();
            if (whereClause != null) {
            	List<String> extraFilters = new ArrayList<String>();  // Hold extra criteria for filtering in-situ data sources prior to SimSearch
	            Expression expr = CCJSqlParserUtil.parseCondExpression(whereClause.toString());
	            ArrayList<SearchSpecs> listConditions = new ArrayList<SearchSpecs>();
	            if (expr != null) {
	            	// Examine condition(s) and specify the necessary similarity search criteria
	            	expr.accept(new ExpressionVisitorAdapter() {
	            		// Also retain several extra filter conditions in SQL as is, beyond the SIMILARITY operator
		                @Override
		                public void visit(OrExpression expr) {
		                	extraFilters.add(expr.toString()); 
		                }
		                @Override
		                public void visit(NotExpression expr) {
		                	extraFilters.add(expr.toString());
		                }
		                @Override
		                public void visit(Between expr) {
		                	extraFilters.add(expr.toString());
		                }
		                @Override
		                public void visit(InExpression inExpression) {
		                	extraFilters.add(expr.toString()); 
		                }
		                @Override
		                protected void visitBinaryExpression(BinaryExpression expr) {
		                    if (expr instanceof LikeExpression) {
		                    	if (expr.getRightExpression().toString().contains("%"))  // Original LIKE condition kept as filter
		                    		extraFilters.add(expr.toString());
		                    	else {	// Handle conditions involving the SIMILARITY operator
		//	                    	System.out.println("left=" + expr.getLeftExpression() + "  op=" +  expr.getStringExpression() + "  right=" + expr.getRightExpression());
			                    	SearchSpecs specs = new SearchSpecs();
			                    	specs.column = expr.getLeftExpression().toString().replaceAll("\'","");
			                    	specs.value =  expr.getRightExpression().toString().replaceAll("\'","");
			                    	// Associate the corresponding weight (if specified in the query)
			                    	if ((weightClause != null) && (weightClause.size() > listConditions.size())) {
			                    		try {
			                    			specs.weights = new Double[1];
			                    			specs.weights[0] = Double.parseDouble(weightClause.get(listConditions.size()).toString());
			                    		} catch(Exception e) {
			                    			System.out.println("NOTICE: Weight values must be real numbers strictly between 0 and 1. Query aborted.");
			                    		}
			                    	}
			                    	listConditions.add(specs);
		                    	}
		                    }
		                    else if (expr instanceof ComparisonOperator) {  // Filters involving =, <>, <, >, <=, >=
		                    	extraFilters.add(expr.toString());
		                    }
		                    super.visitBinaryExpression(expr); 
		                }
		            });
	            }
	            
	            // Query must include at least one similarity condition
	            if (listConditions.isEmpty()) 
	            	System.out.println("Error in SQL statement. SELECT queries must include at least one condition in the WHERE clause involving an existing attribute. Syntax: \n" + Constants.SQL_SELECT_PATTERN);
	            
	            // Apply any extra filters in the search specifications
	            if (extraFilters.size() > 0) {
	            	String strFilter = "(" + String.join(") AND (", extraFilters) + ")";
	            	for (SearchSpecs cond: listConditions) {
	            		cond.filter = strFilter;  // Apply filter in each data source
	            	}
	            }
	            
	            // Append the conditions to the SimSearch request
	            SearchSpecs[] conditions = listConditions.toArray(new SearchSpecs[0]);
	            req.queries = conditions;
            }
            else
            	System.out.println("Error in SQL statement. SELECT queries must include at least one condition in the WHERE clause involving an existing attribute. Syntax: \n" + Constants.SQL_SELECT_PATTERN);
            
        } catch (JSQLParserException e) {
            e.printStackTrace();
            System.out.println("Error in SQL statement. SELECT queries must follow this syntax: \n" + Constants.SQL_SELECT_PATTERN);
        }
	
		return req;
	}
 
	
	/**
	 * Renames a custom SimSearch clause to standard SQL ones (not involved in SimSearch) in order to enable parsing.
	 * @param selectSQL  The originally submitted SQL statement. 
	 * @param origClause  The original SimSearch clause to find.
	 * @param replaceClause  The standard SQL clause to be used as replacement.
	 * @return  The converted SQL statement to parse.
	 */
	private String rename(String selectSQL, String origClause, String replaceClause) {
		// Identify whether the sought clause exists in the SQL statement
		int i = selectSQL.toLowerCase().indexOf(origClause);
		if (i >-1 )  // If found, replace it
			selectSQL = selectSQL.substring(0, i) + replaceClause + selectSQL.substring(i+origClause.length());	
		return selectSQL;
	}
	
}
