import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableProcess {
    public static Configuration conf;
    public static Connection connection;
    public static void getConnect() throws IOException {
        conf=HBaseConfiguration.create();
        try{
            connection=ConnectionFactory.createConnection(conf);
            System.out.println("Connect to HBASE successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void dropTable(String table) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(table);
        if (!admin.tableExists(tableName)) {
            System.out.println("Table '"+tableName + "' does not exist");
            return;
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("Table '"+tableName.toString() + "' has been deleted");
        admin.close();
    }

    public static void createTable(String table, String[] familyNames) throws IOException{
        TableName tableName = TableName.valueOf(table);
        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table '"+tableName.toString() + "' is exist, it will be deleted and recreated");
        }
            System.out.println("start create table '"+table+"'");
            HTableDescriptor tdesc = new HTableDescriptor(tableName);
            for (String familyName : familyNames) {
                tdesc.addFamily(new HColumnDescriptor(familyName));
            }
            admin.createTable(tdesc);
            System.out.println("create table '"+table+"' successfully");
            admin.close();
    }

    public static void addData(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("put " + rowKey + " | " + family + " => "+column + " : " + value + " to table " + tableName + " successfully");
    }

    public static void cellDecoder(Result result) throws IOException {
        for (Cell cell : result.rawCells()) {
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(cell)) +" |"+
                    " colFami: " + new String(CellUtil.cloneFamily(cell)) + " =>"+
                    " col: " + new String(CellUtil.cloneQualifier(cell)) + " ~"+
                    " value: " + new String(CellUtil.cloneValue(cell)) + " @"+
                    " timestamp: " + cell.getTimestamp());
        }
    }

    public static void scan(String tableName) throws IOException {
        System.out.println("show table '"+tableName +"' status:");
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is: " + new String(row));
            cellDecoder(result);
        }
        System.out.println("that's the case & continue next step\n");
        scanner.close();
    }

    public static String[] colFilter(String tableName, String colFamily, String col, String value) throws IOException {
        System.out.println("start filtering table '"+tableName +"' for specific "+colFamily+"~"+col+":"+value+":");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(colFamily),Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<String> ansRowKey = new ArrayList<String>();
        for (Result result : scanner) {
            for (Cell cell : result.rawCells())
                ansRowKey.add(new String(CellUtil.cloneRow(cell)));
        }
        Set set = new HashSet();
        set.addAll(ansRowKey);
        ansRowKey.clear();
        ansRowKey.addAll(set);
        String ans[] = ansRowKey.toArray(new String[ansRowKey.size()]);
        for(int i = 0; i < ans.length; i++)
            System.out.println(ans[i]+" satisfies the filter");
        scanner.close();
        return ans;
    }

    public static void qualFilter(String tableName, String family, String colName) throws IOException {
        System.out.println("start filtering table '" + tableName + "' for specific colName:" + colName + ":");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(colName))));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("filtered student " + new String(CellUtil.cloneRow(cell)) + " get "+new String(CellUtil.cloneValue(cell))+" in "+ colName);
            }
        }
        scanner.close();
    }

    public static void deleteRow(String tableName, String rowKey, String colFami) throws IOException {
        System.out.println("start filtering table '" + tableName + "' to find specific student " + rowKey + ":");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        allFilters.addFilter(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(colFami))));
        allFilters.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowKey)));
        scan.setFilter(allFilters);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            for (Cell cell : result.rawCells()) {
                Delete d = new Delete(result.getRow());
                d.addFamily(Bytes.toBytes(colFami));
                table.delete(d);
            }
            System.out.println("Row " + rowKey +" column family " + colFami +" from table " + tableName + " deleted");
        }
        scanner.close();
    }


    public static void main(String[] args) throws Exception
    {
        getConnect();
        System.out.println(">Q1: Create appropriate tables.");
        System.out.println(">>Step1 4Q1: Create tables Students and Courses.");

        String[] stuNames = {"StuInfo", "Grades"};
        createTable("Students", stuNames);
        String[] courNames = {"C_Info"};
        createTable("Courses", courNames);

        System.out.println(">>Step2 4Q1: Add data.");

        addData("Courses", "123001", "C_Info", "C_Name", "Math");
        addData("Courses", "123001", "C_Info", "C_Credit", "2.0");

        addData("Courses", "123002", "C_Info", "C_Name", "Computer Science");
        addData("Courses", "123002", "C_Info", "C_Credit", "5.0");

        addData("Courses", "123003", "C_Info", "C_Name", "English");
        addData("Courses", "123003", "C_Info", "C_Credit", "3.0");

        addData("Students", "2015001", "StuInfo", "S_Name", "Li Lei");
        addData("Students", "2015001", "StuInfo", "S_Sex", "male");
        addData("Students", "2015001", "StuInfo", "S_Age", "23");
        addData("Students", "2015001", "Grades", "123001", "86");
        addData("Students", "2015001", "Grades", "123003", "69");

        addData("Students", "2015002", "StuInfo", "S_Name", "Han Meimei");
        addData("Students", "2015002", "StuInfo", "S_Sex", "female");
        addData("Students", "2015002", "StuInfo", "S_Age", "22");
        addData("Students", "2015002", "Grades", "123002", "77");
        addData("Students", "2015002", "Grades", "123003", "99");

        addData("Students", "2015003", "StuInfo", "S_Name", "Zhang San");
        addData("Students", "2015003", "StuInfo", "S_Sex", "male");
        addData("Students", "2015003", "StuInfo", "S_Age", "24");
        addData("Students", "2015003", "Grades", "123001", "98");
        addData("Students", "2015003", "Grades", "123002", "95");

        System.out.println(">>>Check Step2 4Q1:");

        scan("Students");
        scan("Courses");

        System.out.println(">>Q1 over.");
        System.out.println(">Q2: Find those who take Computer Science as elective.");
        System.out.println(">>Step1 4Q2: Find corresponding C_No for Computer Science in Courses.");
        String tarRow[] =colFilter("Courses","C_Info","C_Name","Computer Science");
        System.out.println(">>Step1 4Q2: Find the ans in table Students.");
        for(int i = 0; i < tarRow.length; i++)
            qualFilter("Students", "Grades",tarRow[i]);
        System.out.println(">>Q2 over.");

        System.out.println(">Q3: Add new colfami and col with data.");
        HColumnDescriptor newFamliy = new HColumnDescriptor("Contact");
        Admin admin = connection.getAdmin();
        admin.addColumn(TableName.valueOf("Students"), newFamliy);
        admin.close();
        System.out.println("Add new colfami:Contact.");
        addData("Students", "2015001", "Contact", "Email", "lilie@qq.com");
        addData("Students", "2015002", "Contact", "Email", "hmm@qq.com");
        addData("Students", "2015003", "Contact", "Email", "zs@qq.com");
        System.out.println(">>Q3 over.");

        System.out.println(">Q4: Delete course records of a specific student.");
        deleteRow("Students","2015003","Grades");
        System.out.println(">>Q4 over.");

        System.out.println(">Q5: Delete tables created.");
        dropTable("Students");
        dropTable("Courses");
        System.out.println(">>Q5 over.");
        System.out.println("Table processing over.");
        connection.close();
    }
}