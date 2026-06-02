package com.qlangtech.tis.plugin.ontology.chatbi.validation;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.junit.Test;

public class TestPrestoParserDebug {

    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testParseSimpleDecimal() {
        try {
            String sql = "SELECT 14.99";
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions());
            System.out.println("✓ Simple decimal parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ Simple decimal failed: " + e.getMessage());
        }
    }

    @Test
    public void testParseHavingWithDecimal() {
        try {
            String sql = "SELECT Store_City FROM toy_stores GROUP BY Store_City HAVING AVG(Price) < 14.99";
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions());
            System.out.println("✓ HAVING with decimal parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ HAVING with decimal failed: " + e.getMessage());
        }
    }

    @Test
    public void testParseStrToDate() {
        try {
            String sql = "SELECT STR_TO_DATE(Store_Open_Date, '%Y-%m-%d') FROM toy_stores";
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions());
            System.out.println("✓ STR_TO_DATE parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ STR_TO_DATE failed: " + e.getMessage());
        }
    }

    @Test
    public void testParseFullSql() {
        try {
            String sql = """
                    SELECT DISTINCT s.Store_City
                    FROM toy_stores s
                    JOIN toy_inventory i ON s.Store_ID = i.Store_ID
                    JOIN toy_products p ON i.Product_ID = p.Product_ID
                    WHERE STR_TO_DATE(s.Store_Open_Date, '%Y-%m-%d') < '2008-01-01'
                    GROUP BY s.Store_City
                    HAVING AVG(CAST(p.Product_Price AS DECIMAL(10,2))) < 14.99
                    """;
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions());
            System.out.println("✓ Full SQL parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ Full SQL failed: " + e.getMessage());
        }
    }
}
