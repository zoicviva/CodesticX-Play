import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

class TableExtractor:
    
    @staticmethod
    def is_subselect(parsed):
        if not parsed.is_group():
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    @staticmethod
    def extract_from_part(parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if TableExtractor.is_subselect(item):
                    for x in TableExtractor.extract_from_part(item):
                        yield x
                elif item.ttype is Keyword:
                    raise StopIteration
                else:
                    yield item
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True

    @staticmethod
    def extract_table_identifiers(token_stream):
        for item in token_stream:
    
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield str(identifier)
            elif isinstance(item, Identifier):
                yield str(item)
            # It's a bug to check for Keyword here, but in the example
            # above some tables names are identified as keywords...
            elif item.ttype is Keyword:
                yield item.value

    @staticmethod
    def extract_tables(sql):
        stream = TableExtractor.extract_from_part(sqlparse.parse(sql)[0])
        return list(TableExtractor.extract_table_identifiers(stream))
