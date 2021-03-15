from openpyxl import load_workbook
from openpyxl.styles.differential import DifferentialStyle
from openpyxl.styles import Color, PatternFill, Font, Border
from openpyxl.styles import colors
from openpyxl.cell import Cell
import sqlite3
from sqlite3 import Error
import time
import sys
import os.path
from os import path


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def insert_inv_trade(conn, trade):
    sql = ''' INSERT INTO inv_trades(line,side,symbol,qty,price,expiry,strike,pc)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, trade)
    conn.commit()
    return cur.lastrowid


def insert_cece_trade(conn, trade):
    sql = ''' INSERT INTO cece_trades(cece_id,side,symbol,qty,price,expiry,strike,pc)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, trade)
    conn.commit()
    return cur.lastrowid


def insert_matched_trade(conn, cece_id):
    sql = ''' INSERT INTO matched_trades(cece_id)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, cece_id)
    conn.commit()
    return cur.lastrowid


def insert_mapping(conn, mapped):
    sql = ''' INSERT INTO mappings(cece_id, lines)
              VALUES(?,?) '''
    cur = conn.cursor()
    cur.execute(sql, mapped)
    conn.commit()
    return cur.lastrowid


inv_rollup_Fill = PatternFill(start_color='64FF64',
                              end_color='64FF64',
                              fill_type='solid')


def main():
    try:
        workbook_path = input("Provide full path to TJM file (type q to quit): ")
        if workbook_path == 'q':
            sys.exit()
        while path.isfile(workbook_path) == False:
            workbook_path = input("Provide full path to TJM file (type q to exit): ")
            if workbook_path == 'q':
                sys.exit()
            print("Checking: " + workbook_path)
        print("Thank you, working...")

        # database = r"C:\sqlite\db\pythonsqlite.db"
        database = r":memory:"

        sql_create_inv_trades_table = """ CREATE TABLE IF NOT EXISTS inv_trades (
                                        id integer PRIMARY KEY,
                                        line int NOT NULL,
                                        side text NOT NULL,
                                        symbol text NOT NULL,
                                        qty integer NOT NULL,
                                        price float NOT NULL,
                                        expiry text,
                                        strike float NOT NULL,
                                        pc text
                                    ); """

        sql_create_cece_trades_table = """CREATE TABLE IF NOT EXISTS cece_trades (
                                        id integer PRIMARY KEY,
                                        cece_id integer NOT NULL,
                                        side text NOT NULL,
                                        symbol text NOT NULL,
                                        qty integer NOT NULL,
                                        price float NOT NULL,
                                        expiry text,
                                        strike float,
                                        pc text
                                    );"""

        sql_create_matched_trades_table = """CREATE TABLE IF NOT EXISTS matched_trades (
                                        id integer PRIMARY KEY,
                                        cece_id integer NOT NULL
                                    );"""

        sql_create_mappings_table = """CREATE TABLE IF NOT EXISTS mappings (
                                        id integer PRIMARY KEY,
                                        cece_id integer NOT NULL,
                                        lines text NOT NULL
                                    );"""

        # create a database connection
        conn = create_connection(database)

        # create tables
        if conn is not None:
            c = conn.cursor()
            # get the count of tables with the name
            c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='inv_trades' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] != 1: {
                # create projects table
                create_table(conn, sql_create_inv_trades_table)
            }
            # get the count of tables with the name
            c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='cece_trades' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] != 1: {
                # create tasks table
                create_table(conn, sql_create_cece_trades_table)
            }
            # get the count of tables with the name
            c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='matched_trades' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] != 1: {
                # create tasks table
                create_table(conn, sql_create_matched_trades_table)
            }
            # get the count of tables with the name
            c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='mappings' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] != 1: {
                # create tasks table
                create_table(conn, sql_create_mappings_table)
            }

        else:
            print("Error! cannot create the database connection.")

        # insert code to pull tades from excel and add to database

        workbook = load_workbook(filename=workbook_path, data_only=True)
        sheet = workbook.active

        invs = []
        ceces = []

        for i, row in enumerate(sheet.iter_rows(min_row=10, values_only=True)):
            # if inv_trade not already matched
            if (not row[2] is None) and (row[12] is None):
                inv_trade = (i + 10, row[1], row[2], row[3], row[4], str(row[5]), row[6], row[7])
                trade_id = insert_inv_trade(conn, inv_trade)
        ##            else:
        ##                if not row[12] is None:
        ##                    matched = (int(row[12]),)
        ##                    trade_id = insert_matched_trade(conn, matched)

        for row in sheet.iter_rows(min_row=10, values_only=True):
            if not row[25] is None:
                test = conn.cursor()
                test.execute("select count(*) from matched_trades where cece_id ='" + str(row[17]) + "';")
                test_row = test.fetchone()
                if test_row[0] == 0:
                    cece_trade = (row[17], row[23], row[25], row[24], row[29], str(row[26]), row[27], row[28])
                    trade_id = insert_cece_trade(conn, cece_trade)

        workbook.close()

        # select back out summerized inv_trades
        sum = conn.cursor()
        sum.execute(
            "select side,symbol,sum(qty),price,expiry,strike,pc,GROUP_CONCAT(line, ',') AS ids from inv_trades group by side,symbol,price,expiry,strike,pc;")
        sum_row = sum.fetchone()
        while sum_row:
            cece = conn.cursor()
            cece.execute(
                "select side,symbol,qty,price,expiry,strike,pc,cece_id from cece_trades where symbol = '" + sum_row[
                    1] + "';")
            cece_row = cece.fetchone()
            while cece_row:
                if (sum_row[0] == cece_row[0]) and (sum_row[1] == cece_row[1]) and (sum_row[2] == cece_row[2]) and (
                        sum_row[3] == cece_row[3]) and (sum_row[4] == cece_row[4]) and (sum_row[5] == cece_row[5]) and (
                        sum_row[6] == cece_row[6]):
                    mapping = (cece_row[7], str(sum_row[7]))
                    mapping_id = insert_mapping(conn, mapping)
                    matched = (int(cece_row[7]),)
                    insert_matched_trade(conn, matched)
                    break
                cece_row = cece.fetchone()
            sum_row = sum.fetchone()

        # clear cece_trades
        clean_cece = conn.cursor()
        clean_cece.execute("delete from cece_trades;")
        conn.commit()

        # update inv_trades
        lines_remove = conn.cursor()
        lines_remove.execute("select * from mappings;")
        lr_row = lines_remove.fetchone()
        while lr_row:
            liner = []
            liner = str(lr_row[2]).split(',')
            for n in liner:
                inv_del = conn.cursor()
                inv_del.execute("delete from inv_trades where line =" + str(n) + ";")
                conn.commit()
            lr_row = lines_remove.fetchone()

        # update matched_trades
        new_matched = conn.cursor()
        new_matched.execute("insert into matched_trades (cece_id) select cece_id from mappings;")

        # repopulate cece_trades
        for row in sheet.iter_rows(min_row=10, values_only=True):
            if not row[25] is None:
                test = conn.cursor()
                test.execute("select count(*) from matched_trades where cece_id ='" + str(row[17]) + "';")
                test_row = test.fetchone()
                if test_row[0] == 0:
                    cece_trade = (row[17], row[23], row[25], row[24], row[29], str(row[26]), row[27], row[28])
                    trade_id = insert_cece_trade(conn, cece_trade)

        # select trades from inv_trades
        inv = conn.cursor()
        inv.execute("select side,symbol,qty,price,expiry,strike,pc,line from inv_trades;")
        inv_row = inv.fetchone()
        while inv_row:
            cece_single = conn.cursor()
            cece_single.execute(
                "select side,symbol,qty,price,expiry,strike,pc,cece_id from cece_trades where symbol = '" + inv_row[
                    1] + "';")
            cece_single_row = cece_single.fetchone()
            while cece_single_row:
                if (inv_row[0] == cece_single_row[0]) and (inv_row[1] == cece_single_row[1]) and (
                        inv_row[2] == cece_single_row[2]) and (inv_row[3] == cece_single_row[3]) and (
                        inv_row[4] == cece_single_row[4]) and (inv_row[5] == cece_single_row[5]) and (
                        inv_row[6] == cece_single_row[6]):
                    mapping = (cece_single_row[7], str(inv_row[7]))
                    mapping_id = insert_mapping(conn, mapping)
                    matched = (int(cece_single_row[7]),)
                    insert_matched_trade(conn, matched)
                    break
                cece_single_row = cece_single.fetchone()
            inv_row = inv.fetchone()

        # add mappings to matched trades
        workbook2 = load_workbook(filename=workbook_path)
        sheet2 = workbook2.active
        matches = conn.cursor()
        matches.execute("select * from mappings;")
        match_row = matches.fetchone()
        while match_row:
            lines = []
            lines = str(match_row[2]).split(',')
            for n in lines:
                sheet2["M" + str(n)] = match_row[1]
                sheet2["M" + str(n)].fill = inv_rollup_Fill
                sheet2["K" + str(n)] = "=vlookup(M" + str(n) + ",R:V,3,FALSE)"
                sheet2["L" + str(n)] = "=vlookup(M" + str(n) + ",R:V,5,FALSE)"
            match_row = matches.fetchone()

        workbook2.save(filename="C:/Users/logib/Desktop/TJM HA_15 Invoice - Feb 2021-Processed.xlsx")

    except Exception as e:
        print(e)

    # clean out tables
    clean = conn.cursor()
    clean.execute('delete from inv_trades;')
    conn.commit()
    clean.execute('delete from cece_trades;')
    conn.commit()
    clean.execute('delete from matched_trades;')
    conn.commit()
    clean.execute('delete from mappings;')
    conn.commit()
    ##    clean.execute('VACUUM;')
    ##    conn.commit()
    print('Database cleaned. Exiting...')


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))

