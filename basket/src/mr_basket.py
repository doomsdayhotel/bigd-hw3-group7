#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from collections import Counter

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

        # Split the line into fields
        user_id, date, item = line.split(',')
        # Group items by user and session
        yield (user_id, date), item

    
    def reducer_group_items_by_session(self, user_session, items):
        # Collect all items for each user session
        sorted_items = sorted(set(items))  # Convert to set to remove duplicates, then back to list
        # Emit all combinations of items
        for item_pair in combinations(sorted_items, 2):
            yield item_pair, 1
    
    def reducer_count_item_pairs(self, item_pair, counts):
        # Aggregate counts for each item pair
        yield item_pair, sum(counts)

    def mapper_count_pairs(self, item_pair, count):
        # Emit each item with its pair and count
        item1, item2 = item_pair
        yield item1, (item2, count)
        yield item2, (item1, count)
    
    def reducer_find_most_frequent_cooccur(self, item, cooccurs):
        # Find the most frequently co-occurring item for each item
        # Counter takes an iterable, but we want to count pairs (item, count), so use dict
        cooccur_dict = dict(cooccurs)
        # Find the item with the maximum count (most frequent co-occurrence)
        if cooccur_dict:
            cooccur_item, max_count = max(cooccur_dict.items(), key=lambda x: x[1])
            yield item, (cooccur_item, max_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_session_items,
                   reducer=self.reducer_group_items_by_session),
            MRStep(reducer=self.reducer_count_item_pairs),
            MRStep(mapper=self.mapper_count_pairs,
                   reducer=self.reducer_find_most_frequent_cooccur)
        ]
    


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
