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

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs
        """

        user_id, date, item_name = line.split(',')
        yield (user_id, date), item_name

    def reducer_group_by_basket(self, basket, items):
        # Remove duplicate items in basket
        yield basket, list(set(items))

    def mapper_generate_item_pairs(self, basket, items):
        # Generate co-occurring item pairs
        for item_a, item_b in combinations(items, 2):
            yield (item_a, item_b), 1
            yield (item_b, item_a), 1

    def reducer_count_co_occurrence(self, item_pair, counts):
        # Sum counts for each co-occurring item pairs
        yield item_pair[0], (item_pair[1], sum(counts))

    def mapper_reformat_key_value(self, item, co_occurrence):
        # co_occurrence is a tuple of (paired item, co-occurrence count)
        yield item, co_occurrence

    def reducer_find_max_co_occurrence(self, item, co_occurrence):
        # Get the tuple of (paired item, co-occurrence count) where co-occurrence count is highest for item
        most_common = max(co_occurrence, key=itemgetter(1))
        yield item, most_common

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_session_items,
                   reducer=self.reducer_group_by_basket),
            MRStep(mapper=self.mapper_generate_item_pairs,
                   reducer=self.reducer_count_co_occurrence),
            MRStep(mapper=self.mapper_reformat_key_value,
                   reducer=self.reducer_find_max_co_occurrence)
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
