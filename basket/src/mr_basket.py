#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from operator import itemgetter


class MRBasket(MRJob):
    """
    A class to count item co-occurrence in shopping baskets
    """

    def mapper_get_session_items(self, _, line):
        """
        Mapper function that parses each line and extracts purchased items from a shopping session.

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                A line from input file representing a transaction in the form user_id, date, and item_name delimited by
                comma.

            Yields:
                ((user_id, date), item_name): ((str, str), str)
                    A (key, value) pair where key is a tuple of (user_id, date) which identifies a shopping session and
                    value is the item purchased in that session.

                example: (('1000', '15-03-2015'), 'semi-finished bread')
        """
        # Emit the session identifier (user_id, date) and the item name.
        user_id, date, item_name = line.split(',')
        yield (user_id, date), item_name

    def reducer_group_items_by_session(self, session_id, items):
        '''
        Reducer function that groups unique items purchased in a shopping session of a user.

        Parameters:
            session_id: (str, str)
                A tuple of (user_id, date) that represents a shopping session (basket).
            items: list[str]
                A list of items purchased in the given shopping session.

            Yields:
                ((user_id, date), items): ((str, str), list[str])
                    A (key, value) pair where key is a tuple of (user_id, date) which identifies a shopping session and
                    value is the list of item purchased in that session, deduplicated.

                    example: (('1000', '15-03-2015'), ['semi-finished bread', 'whole milk', 'yogurt'])
        '''
        # Removes duplicate items in basket
        yield session_id, list(set(items))
    

    def mapper_item_pair(self, session_id, item_list):
        '''
        Mapper function that generates pairs of items that occur together in the same basket.

        Parameters:
            session_id:
                A tuple of (user_id, date) that represents a shopping session (basket).
            items: list[str]
                A list of unique items in the given shopping session.

            Yields:
                ((item_a, item_b), count):  ((str, str), int)
                    A (key, value) pair where key is a tuple of item names of a co-occurring item pair and value is the
                    count of 1 for each pair.

                example:
                    (('whole milk', 'yogurt'), 1)
                    (('yogurt', 'whole milk'), 1)
        '''
        for item_a, item_b in combinations(item_list, 2):
            yield (item_a, item_b), 1
            yield (item_b, item_a), 1

    def reducer_count_item_pair(self, item_pair, counts):
        """
        Reducer function that counts the number of times each item pair co-occurs across all sessions.

        Parameters:
            item_pair: (str, str)
                A tuple of item names of a co-occurring item pairs.
            counts: list[int]
                A list of counts for each co-occurring item pairs.

            Yields:
                (item1, (item2, total_count)): (str, (str, int))
                    A (key, value) pair where key is the item name, and value is a tuple of co-occurring item name and
                    sum of counts for that pair.

                example: ('yogurt', ('house keeping products', 2))

        """
        yield item_pair[0], (item_pair[1], sum(counts))
    
    
    def reducer_find_most_common_cooccur(self, item, co_occurrence):
        """
        Reducer function that gets the item that most frequently co-occurs with each given item.

        Parameters:
            item: str
                Item name of the item to find the most common co-occurring item for.
            co_occurrence: list[(str, int)]
                List of tuples in the format (co_occurring_item, count) for items that co-occur with input item.

            Yields: (item, cooccur_item): (str, (str, int))
                A (key, value) pair where key is a tuple of the most co-occurring items, and value is its co-occurrence count.

            example: ('chocolates', ('pumpkins', 1)), ('chocolates', ('eggs', 1))
        """
        cooccur_dict = dict(co_occurrence)
        # Find the item with the maximum count (most frequent co-occurrence)
        if cooccur_dict:
            # Find the maximum count of co-occurrences
            max_count = max(cooccur_dict.values())
            # Find all items that have the maximum count
            most_frequent_cooccurs = [(cooccur_item, count) for cooccur_item, count in cooccur_dict.items() if count == max_count]
            # Yield the item and all its most frequent co-occurring items
            for cooccur_item in most_frequent_cooccurs:
                yield item, cooccur_item



    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_session_items,
                reducer=self.reducer_group_items_by_session
            ),
            MRStep(
                mapper=self.mapper_item_pair,
                reducer=self.reducer_count_item_pair
            ),
            MRStep(
                reducer=self.reducer_find_most_common_cooccur
            )
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
