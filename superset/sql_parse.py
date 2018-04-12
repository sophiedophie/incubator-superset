# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, Name

RESULT_OPERATIONS = {'UNION', 'INTERSECT', 'EXCEPT'}
PRECEDES_TABLE_NAME = {'FROM', 'JOIN', 'DESC', 'DESCRIBE', 'WITH'}


# TODO: some sql_lab logic here.
class SupersetQuery(object):
    def __init__(self, sql_statement):
        self.sql = sqlparse.format(sql_statement, reindent=True)
        self._table_names = set()
        self._alias_names = set()
        self._temp_array = []
        self._date_array = []
        self.queries_total_date = 0
        self._is_subquery = False
        # TODO: multistatement support
        logging.info('Parsing with sqlparse statement {}'.format(self.sql))
        # Limitation: sqlparse cannot parse more than 2 join queries.
        # so if query has more than 2 join subqueries, split the query and
        # do the original process for first 1 join and remaining queries.
        self.splitted_query = self.__split_query(self.sql)
        if isinstance(self.splitted_query, list):
            self._is_subquery = True
            del self.splitted_query[0]
            for query_element in self.splitted_query:
                self.__parse_call_extract_token(query_element)

        # 1) if it is not subquery 2) subquery case -> first 2 query (e.g. x join y)
        self._parsed = sqlparse.parse(self.sql)
        for statement in self._parsed:
            self.__extract_from_token(statement)
        self._table_names = self._table_names - self._alias_names

        self.all_select_subquery = self.__clean_duplicated_query(self._temp_array)

        # finally parse subqueries here
        if self.all_select_subquery:
            for subquery in self.all_select_subquery:
                self.__parse_call_extract_token(subquery)

        # calculate whole days
        for duration in self._date_array:
            self.queries_total_date += duration
        print(self.queries_total_date)

    @property
    def tables(self):
        return self._table_names

    def is_select(self):
        return self._parsed[0].get_type() == 'SELECT'

    def stripped(self):
        sql = self.sql
        if sql:
            while sql[-1] in (' ', ';', '\n', '\t'):
                sql = sql[:-1]
            return sql

    @staticmethod
    def __precedes_table_name(token_value):
        for keyword in PRECEDES_TABLE_NAME:
            if keyword in token_value:
                return True
        return False

    @staticmethod
    def __get_full_name(identifier):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            return '{}.{}'.format(identifier.tokens[0].value,
                                  identifier.tokens[2].value)
        return identifier.get_real_name()

    @staticmethod
    def __is_result_operation(keyword):
        for operation in RESULT_OPERATIONS:
            if operation in keyword.upper():
                return True
        return False

    @staticmethod
    def __is_identifier(token):
        return (
            isinstance(token, IdentifierList) or isinstance(token, Identifier))

    def __process_identifier(self, identifier):
        # exclude subselects
        if '(' not in '{}'.format(identifier):
            self._table_names.add(SupersetQuery.__get_full_name(identifier))
            return SupersetQuery.__get_full_name(identifier)

        # store aliases
        if hasattr(identifier, 'get_alias'):
            self._alias_names.add(identifier.get_alias())
        if hasattr(identifier, 'tokens'):
            # some aliases are not parsed properly
            if identifier.tokens[0].ttype == Name:
                self._alias_names.add(identifier.tokens[0].value)
        self.__extract_from_token(identifier)

    def as_create_table(self, table_name, overwrite=False):
        """Reformats the query into the create table as query.

        Works only for the single select SQL statements, in all other cases
        the sql query is not modified.
        :param superset_query: string, sql query that will be executed
        :param table_name: string, will contain the results of the
            query execution
        :param overwrite, boolean, table table_name will be dropped if true
        :return: string, create table as query
        """
        # TODO(bkyryliuk): enforce that all the columns have names.
        # Presto requires it for the CTA operation.
        # TODO(bkyryliuk): drop table if allowed, check the namespace and
        #                  the permissions.
        # TODO raise if multi-statement
        exec_sql = ''
        sql = self.stripped()
        if overwrite:
            exec_sql = 'DROP TABLE IF EXISTS {table_name};\n'
        exec_sql += 'CREATE TABLE {table_name} AS \n{sql}'
        return exec_sql.format(**locals())

    def __calculate_date_period(self, date_array):
        """ Get table's date info and calculate the days they get.

        :param date_array: array
        """
        print(date_array)
        date_from = []
        date_to = []
        date_in = ''
        days_diff = ''
        for date in date_array:
            print(date)
            match_dt = re.findall(r'\d{4}[-/]\d{2}[-/]\d{2}', date)
            split_date = str(match_dt[0])
            if date.count('in') > 0:
                date_in = len(match_dt)
                print(match_dt)
                if date_in > 90:
                    days_diff = 91
                else:
                    days_diff = date_in
                self._date_array.append(days_diff)
                return
            if date.count('=') > 0 and not date.count('<') > 0 and not date.count('>') > 0:
                self._date_array.append(1)
                return
            if date.count('>') > 0:
                date_from = split_date.split('-')
            if date.count('<') > 0:
                date_to = split_date.split('-')    
        if not date_to:
            days_diff = 91
        if not date_from:
            days_diff = 91
        if date_from and date_to:
            if date_from[0] is not date_to[0]:
                days_diff = 91
            days_from = int(date_from[1]) * 30 + int(date_from[2])
            days_to = int(date_to[1]) * 30 + int(date_to[2])
            days_diff = days_to - days_from
            if days_diff is 0:
                days_diff = 1
        self._date_array.append(days_diff)

    def __parse_call_extract_token(self, query):
        """Grouped often called codes to parse tokens
        """
        _parsed = sqlparse.parse(query)
        for parsed_statement in _parsed:
            self.__extract_from_token(parsed_statement)
    
    def __clean_duplicated_query(self, query_array):
        """Remove duplicated subquery.

        Because parser sucks, it sometimes doesn't parse some parts.
        The logic to parse every subquery occurs duplicated value, so clean up here.
        """
        dedup_query_array = []
        for element in query_array:
            match_query = re.findall(r'\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)', str(element))
            if match_query:
                dedup_query_array.append(match_query[0][1:-1])
        return list(set(dedup_query_array))

    def __split_query(self, sql):
        """To split to one join if there are more than 3 join

        It is because sqlparse library only parse one of join query.
        e.g.) x join y join z => x join y and z
        """
        count_word = 'join'
        cached_sql = sql.lower()
        num = cached_sql.count(count_word)
        if num > 1:
            split_array = cached_sql.split(count_word)
            new_array = []
            for n in range(int(round(len(split_array)/2))):
                if len(split_array) > 0:
                    new_array.append(count_word.join(split_array[:2]))
                    del split_array[:2]
                else:
                    new_array.append(split_array[0])
            return new_array
        return sql

    def __get_smallest_select(self, sql):
        """ Split select until it doesn't exist

        """
        lower_sql = sql.lower()
        if lower_sql.count('select') is 0:
            return
        elif lower_sql.count('select') < 2:
            return sql
        elif lower_sql.count('join') > 0:
            splitted_array = lower_sql.split('join')
            for element in splitted_array:
                self.__get_smallest_select(element)
        else:
            self.__get_smallest_select(lower_sql[lower_sql.find('select')+7:])

    def __select_filter(self, item):
        if item.value.lower().count('select') is 1:
            self._temp_array.append(item.value)
        if (item.value.lower().count('select') > 1):
            self._temp_array.append(self.__get_smallest_select(item.value))

    def __find_dt_table(self, lower_token):
        normalized_value = lower_token.replace('"', "'")
        regex = r"(dt.+?\d{4}-\d{2}-\d{2}')"
        if normalized_value.count('dt in') > 0:
            regex = r'dt\sin\s\(([^\(\)]+)\)'
        match_dt = re.findall(regex, normalized_value)
        match_table = re.findall(r"from\s(.+?)\s+where", normalized_value)
        if match_dt and match_table:
            if match_table[0].count('table_name') > 0:
                self.__calculate_date_period(match_dt)

    def __extract_from_token(self, token):
        """ After parse, detect each token and make it small parts.

        """
        if not hasattr(token, 'tokens'):
            return

        # if get simple select query
        lower_token = str(token).lower()
        if lower_token.startswith('select') and lower_token.count('join') is 0:
            self.__find_dt_table(lower_token)

        table_name_preceding_token = False
        
        for item in token.tokens:
            if item.is_group and self._is_subquery:
                self.__select_filter(item)
            
            if item.is_group and not self.__is_identifier(item):                
                self.__select_filter(item)
                self.__extract_from_token(item)

            if item.ttype in Keyword:
                if SupersetQuery.__precedes_table_name(item.value.upper()):
                    table_name_preceding_token = True
                    continue

            if not table_name_preceding_token:
                continue

            if item.ttype in Keyword:
                if SupersetQuery.__is_result_operation(item.value):
                    table_name_preceding_token = False
                    continue
                # FROM clause is over
                break

            if isinstance(item, Identifier):
                self._table_name = self.__process_identifier(item)

            if isinstance(item, IdentifierList):
                for token in item.tokens:
                    if SupersetQuery.__is_identifier(token):
                        self.__process_identifier(token)
