#Oracle schema creator

import cx_Oracle as ora, re, time


ORA_USER = "ORA_USER"
ORA_PASS = "ORA_PASS "
ORA_SID = "ORA_SID"

class SOTAScripts():
    def __init__(self):
        self.tables_rows = []
        self.triggers_rows = []
        self.indexes_rows = []
        self.sequences_rows = []

        self.TABLES_FILENAME = "tables.txt"
        self.DDL_TABLES_FILENAME = "ddl_tables.txt"
        self.DDL_TRIGGERS_FILENAME = "ddl_triggers.txt"
        self.DDL_INDEXES_FILENAME = "ddl_indexes.txt"
        self.DDL_SEQENCES_FILENAME = "ddl_sequences.txt"

        self.cursor = ora.connect(ORA_USER, ORA_PASS, ORA_SID).cursor()
    def getTablesNames(self):
        query = "SELECT table_name FROM USER_TABLES"
        self.tables_rows = self.cursor.execute(query).fetchall()
        return self.tables_rows
    def getTriggersNames(self):
        query = "select trigger_name from all_triggers where owner = '{0}'".format(ORA_USER)
        self.triggers_rows = self.cursor.execute(query).fetchall()
        return self.triggers_rows
    def getIndexesNames(self):
        query = "select index_name from all_indexes where owner = '{0}'".format(ORA_USER)
        self.indexes_rows = self.cursor.execute(query).fetchall()

        return self.indexes_rows
    def getSequencesNames(self):
        query = "SELECT sequence_name FROM all_sequences where sequence_owner = '{0}'".format(ORA_USER)
        self.sequences_rows = self.cursor.execute(query).fetchall()
        return self.sequences_rows 
    #
    def prepareDB(self):
        self.cursor.execute('''
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
        END;''')
        self.cursor.execute('''
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',false);
        END;''')
        self.cursor.execute('''
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
        END;''')
    def writeTablesDDL(self):
        if not self.tables_rows:
            print("No table names! Use getTablesNames() first!")
            return
        filename = time.strftime("%d_%m_%Y-%H:%M:%S_") + self.DDL_TABLES_FILENAME
        with open(filename, 'w') as fd:
            self.prepareDB()
            for table_name in self.tables_rows:
                if not table_name:
                    continue
                rows = self.cursor.execute("select dbms_metadata.get_ddl('TABLE','{0}','{1}') from dual".format(table_name[0], ORA_USER)).fetchall()
                for row in rows:
                    fd.write(row[0].read())
                    fd.write(";\n")
    def writeTriggersDDL(self):
        if not self.triggers_rows:
            print("No table names! Use getTablesNames() first!")
            return
        filename = time.strftime("%d_%m_%Y-%H:%M:%S_") + self.DDL_TRIGGERS_FILENAME
        with open(filename, 'w') as fd:
            self.prepareDB()
            for trigger_name in self.triggers_rows:
                if not trigger_name:
                    continue
                rows = self.cursor.execute("select dbms_metadata.get_ddl('TRIGGER','{0}','{1}') from dual".format(trigger_name[0], ORA_USER)).fetchall()
                for row in rows:
                    fd.write(row[0].read())
                    fd.write(";\n")
    def writeIndexesDDL(self):
        if not self.indexes_rows:
            print("No indexes names! Use getTablesNames() first!")
            return
        filename = time.strftime("%d_%m_%Y-%H_%M_%S-") + self.DDL_INDEXES_FILENAME
        with open(filename, 'w') as fd:
            self.prepareDB()
            for index_name in self.indexes_rows:
                if not index_name:
                    continue
                rows = self.cursor.execute("select dbms_metadata.get_ddl('INDEX','{0}','{1}') from dual".format(index_name[0], ORA_USER)).fetchall()
                for row in rows:
                    fd.write(row[0].read())
                    fd.write(";\n")
    def writeSequencesDDL(self):
        if not self.sequences_rows:
            print("No sequences names! Use getSequencesNames() first!")
            return
        filename = time.strftime("%d_%m_%Y-%H_%M_%S-") + self.DDL_SEQENCES_FILENAME
        with open(filename, 'w') as fd:
            self.prepareDB()
            for sequence_name in self.sequences_rows:
                if not sequence_name:
                    continue
                rows = self.cursor.execute("select dbms_metadata.get_ddl('SEQUENCE','{0}') from dual".format(sequence_name[0])).fetchall()
                for row in rows:
                    fd.write(row[0].read())
                    fd.write(";\n")

def main():
    b = SOTAScripts()
    b.getSequencesNames()
    b.writeSequencesDDL()
if __name__ == "__main__":
    main()