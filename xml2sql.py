from glob import glob
import sqlite3
import sys
from lxml import etree
import configparser as cp
import logging



# SQL_INDEX is the name of the default primary key 
# It's an integer that increments 
# nouvelle insertion dans une table
SQL_INDEX = "sql_index"

# Une balise xml peut contenir du text dans un CDATA
# ou simplement entre deux balises, une colonne SQL
# est donc réservée pour stocker ce texte.
# XML_TEXT étant le nom de cette colonne
XML_TEXT = "xml_text"

# XML_FILE is the name of the SQL column that will contain
# the name of the file which the markup originates from.
XML_FILE = "xml_file"


"""
Returns a dictionary with the content of the ini file
"""
def parse_ini(conf_file: str):
    conf = cp.ConfigParser()
    with open(conf_file, "r") as file:
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
its foreign keys in the dictionaries 'tables', 'primkeys'
and 'forkeys' unless the markup is in 'ignore'
"""
def def_tables(node, conf, ignore, tables, primkeys, forkeys):
    table_name = node.tag.lower()

    if table_name not in ignore:
        foreign = get_foreign_keys(node, ignore, primkeys)
        table_cols = get_columns(node, conf["primary_keys"], foreign)
        primary = get_primary_keys(node, conf["primary_keys"])

        if table_name not in tables:
            tables[table_name] = table_cols
            forkeys[table_name] = foreign
            primkeys[table_name] = primary
        else:
            # We merge new attributes
            tables[table_name] |= table_cols
            forkeys[table_name] |= foreign

    for child in node.getchildren():
        def_tables(child, conf, ignore, tables, primkeys, forkeys)


def create_tables(con, tables, primkeys, forkeys):
    for table_name in tables.keys():
        query = f"""CREATE TABLE IF NOT EXISTS '{table_name}'(
                    %s,
                    PRIMARY KEY (%s)""" %\
                (',    '.join(tables[table_name]),
                 ', '.join(primkeys[table_name]))

        for key in forkeys[table_name].items():
            query += ",\nFOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)" % (
                key[0], key[1][0], key[1][1])

        query += ")"
        con.execute(query)


def get_parent_key(tables, node, forkey):
    parent = node.getparent()
    table, key = forkey

    while parent is not None and parent.tag not in tables:
        parent = parent.getparent()
        

    if parent is None or parent.tag != table or key not in parent.attrib:
        return ""
    else:
        if key == SQL_INDEX:
            return int(parent.attrib[key])
        return parent.attrib[key]


def fill_tables(con, node, tables, primkeys, forkeys, file = None):
    table_name = node.tag.lower()

    if table_name == XML_FILE:
        file = node.attrib["file"]
    elif table_name in tables:
        values = []
        cols = tables[table_name]

        for col in cols:
            if col in node.attrib:
                if col == SQL_INDEX:
                    values.append(int(node.attrib[col]))
                else:
                    values.append(node.attrib[col])

            elif col in forkeys[table_name]:
                values.append(get_parent_key(
                    tables, node, forkeys[table_name][col]))

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
        fill_tables(con, child, tables, primkeys, forkeys, file)


def merge_xml_files(files: set[str]):
    root = etree.fromstring('<any2vtom_root></any2vtom_root>')
    for file in files:
        root.append(etree.fromstring(f"<{XML_FILE} file='{file}'></{XML_FILE}>"))
        root.find(XML_FILE).append(parse_xml(file))
    return root


def generate_tables(con: sqlite3.Connection, conf: cp.ConfigParser, files: set[str]):
    ignore = set(conf["DEFAULT"]["ignore"].lower().split()) | {XML_FILE, "any2vtom_root"}

    root = merge_xml_files(files)
    print(etree.tostring(root, pretty_print=True))
    
    # tables['table'] = {'colonne1', 'colonne2', ...}
    # Liste des colonnes pour chaque table
    tables: dict[str, set] = dict()

    # primkeys['table'] = {'colonne1', ...}
    # Liste des clés primaires pour chaque table
    primkeys: dict[str, set] = dict()
    
    # forkeys['table'] = {'colonne2': ('table_parent', 'clé_primaire_parent')}
    # Liste des clés étrangères pour chaque table
    forkeys: dict[str, dict[str, tuple[str, str]]] = dict()

    def_tables(root, conf, ignore, tables, primkeys, forkeys)
    create_tables(con, tables, primkeys, forkeys)
    fill_tables(con, root, tables, primkeys, forkeys)

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
        raise ValueError("xml: Nombre d'arguments incorrect")

    main(sys.argv[1])
