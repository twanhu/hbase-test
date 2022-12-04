package com.twanhu.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HBaseDML {

    public static void main(String[] args) throws IOException {
        // 测试添加数据
//        putCell("bigdata", "student", "1004", "info", "age", "16");

        // 测试读取数据
//        getCells("bigdata", "student", "1003", "info", "name");

        // 测试扫描数据
//        scanRows("bigdata", "student", "1001", "1005");

        // 测试带过滤的扫描
//        filterScan("bigdata", "student", "1001", "1005", "info", "name", "gonggong");

        // 测试删除数据
//        deleteColumn("bigdata", "student", "1001", "info", "name");

        // 其他代码
        System.out.println("其他代码");

        // 关闭连接
        HBaseConnection.closeConnection();
    }

    // 静态属性，获取HBase连接
    public static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       主键
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        值
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        // 1、获取Tablel : 对HBase的 数据 进行管理的API
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2、调用相关方法插入数据
        // 2.1、创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 2.2、给put对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        // 2.3、将对象写入对应的方法
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3、关闭table
        table.close();
    }

    /**
     * 读取数据，最多只能读取一行
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       主键
     * @param columnFamily 列族名称
     * @param columnName   列名
     */
    public static void getCells(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1、获取table : 对HBase的 数据 进行管理的API
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2、创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果直接调用get方法读取数据，此时读一整行数据
        // 如果想读取某一列的数据，需要添加对应的参数
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // 设置读取数据的版本，所有版本
        get.readAllVersions();

        try {
            // 读取数据，得到result对象
            Result result = table.get(get);
            // 处理数据
            Cell[] cells = result.rawCells();

            // 测试方法：直接把读取的数据打印到控制台
            // 在实际开发中，需要再额外写方法处理对应数据
            // cell : 单元格,存储数据比较底层（cell中的数据是没有数据类型概念的，都是未解析的字节数组）
            for (Cell cell : cells) {
                // cell存储数据比较底层
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" +
                        new String(CellUtil.cloneQualifier(cell)) + "-(" + sdf.format(new Date(cell.getTimestamp())) + ")-" +
                        new String(CellUtil.cloneValue(cell)) + "\t");
            }
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭table
        table.close();
    }

    /**
     * 扫描数据
     *
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param startRow  开始的row
     * @param stopRow   结束的row
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        // 1、获取table : 对HBase的 数据 进行管理的API
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2、创建scan对象
        Scan scan = new Scan();

        // 如果此时直接调用会直接扫描整张表
        // 添加参数来控制扫描的数据
        // 开始的row，默认包含
        scan.withStartRow(Bytes.toBytes(startRow));
        // 结束的row，默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));

        try {
            // 读取多行数据，获取scanner
            ResultScanner scanner = table.getScanner(scan);

            // result : 记录一行数据，cell数组
            // ResultScanner : 记录多行数据，result的数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                // cell : 单元格,存储数据比较底层（cell中的数据是没有数据类型概念的，都是未解析的字节数组）
                for (Cell cell : cells) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-(" + sdf.format(new Date(cell.getTimestamp())) + ")-" +
                            new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3、关闭table
        table.close();
    }

    /**
     * 带过滤的扫描
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param startRow     开始row
     * @param stopRow      结束row
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        value值
     */
    public static void filterScan(String namespace, String tableName, String startRow, String stopRow, String columnFamily, String columnName, String value) throws IOException {
        // 1、获取table : 对HBase的 数据 进行管理的API
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2、创建scan对象
        Scan scan = new Scan();

        // 如果此时直接调用会直接扫描整张表
        // 添加参数来控制扫描的数据
        // 开始的row，默认包含
        scan.withStartRow(Bytes.toBytes(startRow));
        // 结束的row，默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));

        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // 创建过滤器
        // (1)、结果只保留当前列的数据，单列过滤扫描
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );

        // (2)、结果保留整行数据，整行过滤扫描
        // 结果同时会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );

        filterList.addFilter(singleColumnValueFilter);

        // 添加过滤
        scan.setFilter(filterList);

        try {
            // 读取多行数据，获取scanner
            ResultScanner scanner = table.getScanner(scan);

            // result : 记录一行数据，cell数组
            // ResultScanner : 记录多行数据，result的数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                // cell : 单元格,存储数据比较底层（cell中的数据是没有数据类型概念的，都是未解析的字节数组）
                for (Cell cell : cells) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-(" + sdf.format(new Date(cell.getTimestamp())) + ")-" +
                            new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3、关闭table
        table.close();
    }

    /**
     * 删除一行中的某一列数据
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       主键
     * @param columnFamily 列族
     * @param columnName   列名
     */
    public static void deleteColumn(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1、获取table : 对HBase的 数据 进行管理的API
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2.创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 添加列信息
        // addColumn : 删除最新的一个版本
//        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // addColumns : 删除所有版本
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3、关闭table
        table.close();
    }

}





























