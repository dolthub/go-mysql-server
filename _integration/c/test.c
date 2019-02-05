#include <my_global.h>
#include <mysql.h>

#include <string.h>
#include <assert.h>

#define TEST(s1, s2) do { printf("'%s' =?= '%s'\n", s1, s2); assert(0 == strcmp(s1, s2)); } while(0)

static void finish_with_error(MYSQL *con)
{
    fprintf(stderr, "%s\n", mysql_error(con));
    mysql_close(con);
    exit(1);
}

int main(int argc, char **argv)
{
    MYSQL *con = NULL;
    MYSQL_RES *result = NULL;
    MYSQL_ROW row;

    int n = 0;
    const int expected_num_records = 4;
    const char *expected_name[expected_num_records] = {
        "John Doe\0",
        "John Doe\0",
        "Jane Doe\0",
        "Evil Bob\0"
    };
    const char *expected_email[expected_num_records] = {
        "john@doe.com\0",
        "johnalt@doe.com\0",
        "jane@doe.com\0",
        "evilbob@gmail.com\0"
    };

    printf("MySQL client version: %s\n", mysql_get_client_info());

    con = mysql_init(NULL);
    if (con == NULL) {
        finish_with_error(con);
    }

    if (mysql_real_connect(con, "127.0.0.1", "root", "", "mydb", 3306, NULL, 0) == NULL) {
        finish_with_error(con);
    }

    if (mysql_query(con, "SELECT name, email FROM mytable")) {
        finish_with_error(con);
    }

    result = mysql_store_result(con);
    if (result == NULL) {
        finish_with_error(con);
    }

    while ((row = mysql_fetch_row(result))) {
        TEST(expected_name[n], row[0]);
        TEST(expected_email[n], row[1]);
        ++n;
    }
    assert(expected_num_records == n);

    mysql_free_result(result);
    mysql_close(con);

    return 0;
}