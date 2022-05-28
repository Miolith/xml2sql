from glob import glob
import sqlite3
import sys
from lxml import etree
import configparser as cp
import logging



# SQL_INDEX is the name of the default primary key 
# This column is an integer that increments by 1 with each new
# instance of a markup
SQL_INDEX = "sql_index"

# A markup can contain text inside it or inside a CDATA,
# an SQL column is thus used to store this text.
# Its name is XML_TEXT
XML_TEXT = "xml_text"

# XML_FILE is the name of the SQL column that will contain
# the name of the file which the markup originates from.
XML_FILE = "xml_file"


"""
Returns a dictionary with the content of the ini file
"""
def parse_ini(conf_file: str):
    conf = cp.ConfigParser()
    with open(conf_file, "r"):
        conf.read(conf_file)
    return conf


"""
Returns a dictionary with the content of the XML file
"""
def parse_xml(file: str):
    return etree.parse(file).getroot()


def get_columns(node, primary_keys, foreign) -> set:
    table_cols = {SQL_INDEX} if node.tag not in primary_keys else set()
    table_cols |= set(node.attrib) | set(foreign) | {XML_TEXT, XML_FILE}
    return table_cols


def get_foreign_keys(node, ignore, primkeys) -> dict:
    forkeys = dict()
    parent = node.getparent()

    while parent is not None and parent.tag.lower() in ignore:
        parent = parent.getparent()

    if parent is not None:
        forkeys = {parent.tag.lower()+'_'+key: (
            parent.tag.lower(), key) for key in primkeys[parent.tag.lower()]}

    return forkeys


def get_primary_keys(node, primconf, index=dict()) -> set:
    table_name = node.tag.lower()

    if table_name in primconf:
        primkeys = set(primconf[table_name].lower().split())
    else:
        index[table_name] = index.get(table_name, 0) + 1
        primkeys = {SQL_INDEX}
        node.attrib[SQL_INDEX] = str(index[table_name])

    return primkeys


"""
For each markup, put its attributes, its primary keys and
its foreign keys in the dictionaries 'columns', 'primkeys'
and 'forkeys' unless the markup is in 'ignore'
"""
def def_tables(node, conf, ignore, columns, primkeys, forkeys):
    table_name = node.tag.lower()

    if table_name not in ignore:
        foreign = get_foreign_keys(node, ignore, primkeys)
        table_cols = get_columns(node, conf["primary_keys"], foreign)
        primary = get_primary_keys(node, conf["primary_keys"])

        if table_name not in columns:
            columns[table_name] = table_cols
            forkeys[table_name] = foreign
            primkeys[table_name] = primary
        else:
            # We merge new attributes
            columns[table_name] |= table_cols
            forkeys[table_name] |= foreign

    for child in node.getchildren():
        def_tables(child, conf, ignore, columns, primkeys, forkeys)


def create_tables(con, columns, primkeys, forkeys):
    for table_name in columns.keys():
        query = f"""CREATE TABLE IF NOT EXISTS '{table_name}'(
                    %s,
                    PRIMARY KEY (%s)""" %\
                (',    '.join(columns[table_name]),
                 ', '.join(primkeys[table_name]))

        for key in forkeys[table_name].items():
            query += ",\nFOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)" % (
                key[0], key[1][0], key[1][1])

        query += ")"
        con.execute(query)


def get_parent_key(columns, node, forkey):
    parent = node.getparent()
    table, key = forkey

    while parent is not None and parent.tag not in columns:
        parent = parent.getparent()
        

    if parent is None or parent.tag != table or key not in parent.attrib:
        return ""
    else:
        if key == SQL_INDEX:
            return int(parent.attrib[key])
        return parent.attrib[key]


def fill_tables(con, node, columns, primkeys, forkeys, file = None):
    table_name = node.tag.lower()

    if table_name == XML_FILE:
        file = node.attrib["file"]
    elif table_name in columns:
        values = []
        cols = columns[table_name]

        for col in cols:
            if col in node.attrib:
                if col == SQL_INDEX:
                    values.append(int(node.attrib[col]))
                else:
                    values.append(node.attrib[col])

            elif col in forkeys[table_name]:
                values.append(get_parent_key(
                    columns, node, forkeys[table_name][col]))

            elif col == XML_TEXT:
                values.append(node.text)

            elif col == XML_FILE:
                values.append(file)

            else:
                values.append('')

        query = f"INSERT INTO `{table_name}`(%s) VALUES (%s)" %\
                (', '.join(cols), ", ".join(['?'] * len(values)))
        con.execute(query, values)

    for child in node.getchildren():
        fill_tables(con, child, columns, primkeys, forkeys, file)


def merge_xml_files(files: set[str]):
    root = etree.fromstring('<any2vtom_root></any2vtom_root>')
    for file in files:
        root.append(etree.fromstring(f"<{XML_FILE} file='{file}'></{XML_FILE}>"))
        root.find(XML_FILE).append(parse_xml(file))
    return root


def generate_tables(con: sqlite3.Connection, conf: cp.ConfigParser, files: set[str]):
    ignore = set(conf["DEFAULT"]["ignore"].lower().split()) | {XML_FILE, "any2vtom_root"}

    root = merge_xml_files(files)
    
    # columns['table'] = {'column1', 'column2', ...}
    # Set of (almost) all the columns for each table
    columns: dict[str, set] = dict()

    # primkeys['table'] = {'column1', ...}
    # Set of all the primary keys for each table
    primkeys: dict[str, set] = dict()
    
    # forkeys['table'] = {'column1': ('parent_table', 'parent_primary_key')}
    # Set of all the foreign keys for each table
    forkeys: dict[str, dict[str, tuple[str, str]]] = dict()

    def_tables(root, conf, ignore, columns, primkeys, forkeys)
    create_tables(con, columns, primkeys, forkeys)
    fill_tables(con, root, columns, primkeys, forkeys)

    con.commit()

def connect_to_db(database: str = "output.db") -> sqlite3.Connection:
    con = sqlite3.connect(database)
    con.row_factory = sqlite3.Row
    logger = logging.getLogger()
    con.set_trace_callback(logger.debug)
    return con


def main(conf_file: str):
    conf = parse_ini(conf_file)
    con = connect_to_db("output/output.db")

    file_list = set()

    for files in conf["DEFAULT"]["files"].split(' '):
        for file in glob(files):
            file_list.add(file)

    generate_tables(con, conf, file_list)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("xml: Incorrect amount of parameters")

    main(sys.argv[1])
