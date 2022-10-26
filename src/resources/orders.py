
import pymysql
import json
from resources.base_resource import Base_Resource


class Orders(Base_Resource):

    def __init__(self):
        self.db_schema = 'classicmodels'
        self.db_table = 'orders'
        self.db_table_full_name = self.db_schema + "." + self.db_table

    def get_full_table_name(self):
        return self.db_schema + "." + self.db_table

    def _get_connection(self):
        """
        # DFF TODO There are so many anti-patterns here I do not know where to begin.
        :return:
        """

        # DFF TODO OMG. Did this idiot really put password information in source code?
        # Sure. Let's just commit this to GitHub and expose security vulnerabilities
        #
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="dbuserdbuser",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return conn



    def get_resource_by_id(self, id):
        """

        :param id: The 'primary key' of the resource instance relative to the collection.
        :return: The resource or None if not found.
        """
        sql = "select * from " + self.get_full_table_name() + " where orderNumber = %s"
        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, (id))
        

        if res == 1:
            result = cursor.fetchone()
        else:
            result = None
        cursor.close()
        return result


    def get_by_template(self,
                        path=None,
                        template=None,
                        field_list=None,
                        limit=None,
                        offset=None):
        """
        This is a logical abstraction of an SQL SELECT statement.

        Ignore path for now.

        Assume that
            - template is {'customerNumber': 101, 'status': 'Shipped'}
            - field_list is ['customerNumber', 'orderNumber', 'status', 'orderDate']
            - self.get_full_table_name() returns 'classicmodels.orders'
            - Ignore limit for now
            - Ignore offset for now

        This method would logically execute

        select customerNumber, orderNumber, status, orderDate
            from classicmodels.orders
            where
                customerNumber=101 and status='Shipped'

        :param path: The relative path to the resource. Ignore for now.
        :param template: A dictionary of the form {key: value} to be converted to a where clause
        :param field_list: The subset of the fields to return.
        :param limit: Limit on number of rows to return.
        :param offset: Offset in the list of matching rows.
        :return: The rows matching the query.
        """
        column_name = []
        result_list = []
        column_name = field_list
        select_place_holder = ", ".join(["{}"] * len(field_list))

        # where conditions
        where_length = len(template)
        if where_length == 0:
            where_str = ""
        else:
            where_str = " where " + " and ".join(["{} = %s"] * where_length)
            for i in template.items():
                column_name.append(str(i[0]))
                result_list.append(str(i[1]))

        # limit statement
        if limit == None:
            limit_str = ""
        else:
            limit_str = " limit %s"
            result_list.append(limit)

        arguments = tuple(result_list)
        sql_row = "select " + select_place_holder + " from " + self.get_full_table_name() + where_str + limit_str
        sql = sql_row.format(*column_name)
        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, args=arguments)
        

        if res != 0:
            result = cursor.fetchall()
        else:
            result = None
        cursor.close()
        return result       

    
    def create(self, new_resource):
        """

        Assume that
            - new_resource is {'customerNumber': 101, 'status': 'Shipped'}
            - self.get_full_table_name() returns 'classicmodels.orders'

        This function would logically perform

        insert into classicmodels.orders(customerNumber, status)
            values(101, 'Shipped')

        :param new_resource: A dictionary containing the data to insert.
        :return: Returns the values of the primary key columns in the order defined.
            In this example, the result would be [101]
        """
        result_list = []
        # insert
        insert_place_holder = ", ".join(["{}"] * len(new_resource))
        column_name = list(new_resource.keys())
        if ('orderNumber' not in column_name or 'orderDate' not in column_name
            or 'requiredDate' not in column_name
            or 'status' not in column_name or 'customerNumber' not in column_name):
            raise Exception('All columns except for the \'shippedDate\', \'conments\' column should not be NONE!')

        # value
        value_holder = ", ".join(["%s"] * len(new_resource))
        result_list.extend(list(new_resource.values()))


        arguments = tuple(result_list)
        sql_row = "insert into " + self.get_full_table_name() + "(" + insert_place_holder + ") " + "values(" + value_holder + ")"
        sql = sql_row.format(*column_name)


        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, args=arguments)

        if res == 1:
            result = cursor.fetchone()
        else:
            result = None
        
        cursor.close()
        result_final = new_resource.get("orderNumber",None)    
        return str(result_final)


    def update_resource_by_id(self, id, new_values):
        """
        This is a logical abstraction of an SQL UPDATE statement.

        Assume that
            - id is 30100
            - new_values is {'customerNumber': 101, 'status': 'Shipped'}
            - self.get_full_table_name() returns 'classicmodels.orders'

        This method would logically execute.

        update classicmodels.orders
            set customerNumber=101, status=shipped
            where
                orderNumber=30100


        :param id: The 'primary key' of the resource to update
        :new_values: A dictionary defining the columns to update and the new values.
        :return: 1 if a resource was updated. 0 otherwise.
        """

        # set
        column_name = []
        result_list = []
        set_length = len(new_values)
        if set_length == 0:
            set_str = ""
        else:
            set_str = ", ".join(["{} = %s"] * set_length)
            for i in new_values.items():
                column_name.append(str(i[0]))
                result_list.append(str(i[1])) # type  attention!

        # where
        where_str = " where orderNumber = %s"
        result_list.append(id)


        arguments = tuple(result_list)
        sql_row = "update " + self.get_full_table_name() + " set " + set_str + where_str
        sql = sql_row.format(*column_name)
        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, arguments)

        if res == 1:
            result = cursor.fetchone()
            cursor.close()
            return 1
        else:
            cursor.close()
            return 0


    def delete_resource_by_id(self, id):
        """
        This is a logical abstraction of an SQL DELETE statement.

        Assume that
            - id is 30100
            - new_values is {'customerNumber': 101, 'status': 'Shipped'}

        This method would logically execute.

        delete from classicmodels.orders
            where
                orderNumber=30100


        :param id: The 'primary key' of the resource to delete
        :return: 1 if a resource was deleted. 0 otherwise.
        """
        # delete with primary key
        sql = "delete from " + self.get_full_table_name() + " where orderNumber = %s"

        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, (id))

        if res == 1:
            result = cursor.fetchone()
            cursor.close()
            return 1
        else:
            cursor.close()
            return 0          


if __name__ == "__main__":
    orders_res = Orders()

    #t_h = orders_res.get_by_template(path=None,limit=None, offset=None,template = {'customerNumber': 101, 'status': 'Shipped'},field_list = ['customerNumber', 'orderNumber', 'status', 'orderDate'])
    #t_h = orders_res.get_by_template(path=None,limit=None, offset=None,template = {'customerNumber': 415, 'status': 'Shipped'},field_list = ['customerNumber', 'orderNumber'])

    #res = []
    #for i in t_h:
    #    if i.get('orderDate',None) != None:
    #        i['orderDate'] = str(i['orderDate'])
    #    res.append(i)
    #print(json.dumps(res, indent=2))

    # t_2 = orders_res.update_resource_by_id(new_values = {'customerNumber': 103, 'status': 'shipped','orderDate':'2003-01-07'},id = 10100)
    # print(json.dumps(t_2, indent=2))
    # update classicmodels.orders set customerNumber=363, status='shipped',orderDate = '2003-01-06' where orderNumber=10100;


    #t_3 = orders_res.create(new_resource = {'orderNumber': 60430, 'orderDate': '2009-07-21','requiredDate': '2009-08-21','shippedDate': '2009-07-30','status':'shipped','customerNumber': 103})
    #print(json.dumps(t_3, indent=2))
    # delete from classicmodels.orders where orderNumber=60430;