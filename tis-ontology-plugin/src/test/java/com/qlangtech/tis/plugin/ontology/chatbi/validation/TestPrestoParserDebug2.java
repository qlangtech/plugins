package com.qlangtech.tis.plugin.ontology.chatbi.validation;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.junit.Test;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment;

public class TestPrestoParserDebug2 {

    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testParseWithDecimalAsDouble() {
        try {
            ParsingOptions options = new ParsingOptions(DecimalLiteralTreatment.AS_DOUBLE);
            String sql = "SELECT 14.99";
            Statement statement = SQL_PARSER.createStatement(sql, options);
            System.out.println("✓ Decimal as DOUBLE parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ Decimal as DOUBLE failed: " + e.getMessage());
        }
    }

    @Test
    public void testParseWithDecimalAsDecimal() {
        try {
            ParsingOptions options = new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL);
            String sql = "SELECT 14.99";
            Statement statement = SQL_PARSER.createStatement(sql, options);
            System.out.println("✓ Decimal as DECIMAL parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ Decimal as DECIMAL failed: " + e.getMessage());
        }
    }

    @Test
    public void testFullSqlWithDecimalAsDouble() {
        try {
            ParsingOptions options = new ParsingOptions(DecimalLiteralTreatment.AS_DOUBLE);
            String sql = """
                    SELECT DISTINCT s.Store_City
                    FROM toy_stores s
                    JOIN toy_inventory i ON s.Store_ID = i.Store_ID
                    JOIN toy_products p ON i.Product_ID = p.Product_ID
                    WHERE STR_TO_DATE(s.Store_Open_Date, '%Y-%m-%d') < '2008-01-01'
                    GROUP BY s.Store_City
                    HAVING AVG(CAST(p.Product_Price AS DECIMAL(10,2))) < 14.99
                    """;
            Statement statement = SQL_PARSER.createStatement(sql, options);
            System.out.println("✓ Full SQL with AS_DOUBLE parsed successfully");
        } catch (Exception e) {
            System.err.println("✗ Full SQL with AS_DOUBLE failed: " + e.getMessage());
        }
    }
}
