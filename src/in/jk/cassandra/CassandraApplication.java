package in.jk.cassandra;

import java.util.List;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraApplication {

	public static void main(String[] args) {

		CassandraConnector cassandraConnector = new CassandraConnector();
		Session session = cassandraConnector.openSession();
		System.out.println("Session :: " + session);


		// CQL Query to create Keyspace
		String keyspacequery = "CREATE KEYSPACE google_employee WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};";
		session.execute(keyspacequery);
        System.out.println("google_employee Keyspace is created  ");
		session.execute("USE google_employee");
		// CQL Query for creating table
		String createquery = "Create table employee ( empId int PRIMARY Key, name varchar ,company varchar ,adddress varchar)";
		session.execute(createquery);

		// CQL Query for adding data in employee table
		int empId = 3;
		String name = "JK";
		String company = "Google";
		String address = "India";
		String query = "Insert into employee (empId,name,company,adddress) values(" + empId + ",'" + name + "','"
				+ company + "','" + address + "')";
		
		ResultSet resultSet0 = session.execute(query);
		Row row0 = resultSet0.one();
		System.out.println("Employee Record Added");
		System.out.println(row0);
		System.out.println();

		String selectQuery = "Select * from employee";
		String queryWithWhere = "Select * from employee  where empId=?";

		ResultSet resultSet = session.execute(selectQuery);
		Row row = resultSet.one();

		System.out.println("Employee Id   :" + row.getInt("empId"));
		System.out.println("Employee Name :" + row.getString("name"));
		System.out.println("Company       :" + row.getString("company"));
		System.out.println("Address       :" + row.getString("adddress"));

		System.out.println();

		ResultSet resultSet1 = session.execute(queryWithWhere, empId);
		Row row1 = resultSet1.one();

		System.out.println("Employee Id   :" + row1.getInt("empId"));
		System.out.println("Employee Name :" + row1.getString("name"));
		System.out.println("Company       :" + row1.getString("company"));
		System.out.println("Address       :" + row1.getString("adddress"));

		System.out.println();

		String deleteQuery = "Delete from employee  where empId=?";

		ResultSet resultSet2 = session.execute(deleteQuery, empId);

		List<ExecutionInfo> list = resultSet2.getAllExecutionInfo();
		System.out.println("Record Deleted :: " + list.size());
		System.out.println("Record Deleted Succussfully ..");
		System.out.println("Query Executed Succussfully . ");

		session.close();

	}

}
